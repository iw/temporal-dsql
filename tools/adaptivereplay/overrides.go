package adaptivereplay

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Override sets a law's parameter for a replay, in place of the value the trace recorded.
type Override struct {
	// Controller names the controllers it applies to: those it names, and those whose names
	// start with it and an underscore, as worker_count does each queue's.
	Controller string
	// Field is a field of the law's parameters: the Envelope of persistence_capacity, the
	// Params of the others, and for the controllers the signal plane drives, the
	// envelope's MaxStepFrac too.
	Field, Value string
}

var errOverrideSyntax = errors.New("an override is controller.Field=value")

// ParseOverride reads an override written as controller.Field=value.
func ParseOverride(s string) (Override, error) {
	target, value, ok := strings.Cut(s, "=")
	dot := strings.LastIndex(target, ".")
	if !ok || dot <= 0 || dot == len(target)-1 {
		return Override{}, fmt.Errorf("%w, not %q", errOverrideSyntax, s)
	}
	return Override{Controller: target[:dot], Field: target[dot+1:], Value: value}, nil
}

func (o Override) String() string {
	return o.Controller + "." + o.Field + "=" + o.Value
}

func (o Override) appliesTo(controller string) bool {
	return controller == o.Controller || strings.HasPrefix(controller, o.Controller+"_")
}

func overridden(overrides []Override, controller, field string) bool {
	return slices.ContainsFunc(overrides, func(o Override) bool {
		return o.appliesTo(controller) && o.Field == field
	})
}

// apply sets each override for controller on the first of targets, pointers to structs,
// that has its field.
func apply(overrides []Override, controller string, targets ...any) error {
	for _, o := range overrides {
		if !o.appliesTo(controller) {
			continue
		}
		set := false
		for _, target := range targets {
			ok, err := setField(reflect.ValueOf(target).Elem(), o.Field, o.Value)
			if err != nil {
				return fmt.Errorf("%s: %w", o, err)
			}
			if ok {
				set = true
				break
			}
		}
		if !set {
			return fmt.Errorf("%s: %s has no parameter %s; it has %s", o, controller, o.Field, strings.Join(parameters(targets), ", "))
		}
	}
	return nil
}

var durationType = reflect.TypeFor[time.Duration]()

// setField sets a struct's field from text, and returns false when it has no such field
// that holds a number, a duration or a bool.
func setField(s reflect.Value, name, text string) (bool, error) {
	f := s.FieldByName(name)
	if !f.IsValid() || !f.CanSet() || !settable(f.Type()) {
		return false, nil
	}
	switch {
	case f.Type() == durationType:
		d, err := time.ParseDuration(text)
		if err != nil {
			return false, err
		}
		f.SetInt(int64(d))
	case f.Kind() == reflect.Bool:
		b, err := strconv.ParseBool(text)
		if err != nil {
			return false, err
		}
		f.SetBool(b)
	case f.CanInt():
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return false, err
		}
		f.SetInt(n)
	default:
		x, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return false, err
		}
		f.SetFloat(x)
	}
	return true, nil
}

func settable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// parameters lists the fields of targets an override can set.
func parameters(targets []any) []string {
	var names []string
	for _, target := range targets {
		t := reflect.TypeOf(target).Elem()
		for i := range t.NumField() {
			if f := t.Field(i); f.IsExported() && settable(f.Type) {
				names = append(names, f.Name)
			}
		}
	}
	return names
}
