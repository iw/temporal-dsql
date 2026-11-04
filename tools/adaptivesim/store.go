package adaptivesim

import (
	"math/rand/v2"
	"time"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/gate"
)

// failure is how a call or a statement failed, as the code above the store sees it.
type failure int

const (
	ok failure = iota
	// unavailable is a failure the store returns as Unavailable: a query cut off at its
	// deadline, an acquire that ran out of time, or a refusal the common store flattens.
	unavailable
	// exhausted is ResourceExhausted with SYSTEM_OVERLOADED: the plugin didn't attempt the
	// call, because the gate refused it or DSQL rejected its connection.
	exhausted
	// limited is ResourceExhausted with PERSISTENCE_LIMIT: the persistence rate limiter
	// rejected the call.
	limited
	// timedOut is the context's deadline passing outside the store, such as while a write
	// waited for its shard.
	timedOut
)

// connectResult is how the plugin's connector ended.
type connectResult int

const (
	connected connectResult = iota
	// connectRefused is the gate refusing the caller, which the plugin reports as a
	// throttled fact and returns as ResourceExhausted.
	connectRefused
	// connectRejected is DSQL rejecting the connection the caller waited for, which the
	// plugin also reports as a throttled fact and returns as ResourceExhausted.
	connectRejected
	// connectTimedOut is the caller's deadline passing while it waited for the pool.
	connectTimedOut
)

// acquireFactsInterval is how often the plugin reports its acquires as facts.
const acquireFactsInterval = time.Second

type (
	// store is the plugin's database handle behind database/sql, which keeps no idle
	// connections: each statement outside a transaction, and each transaction, takes a
	// connection from the connector and gives it back. The connector admits the caller
	// through the gate, when there is one, then acquires from the pool.
	store struct {
		l      *loop
		net    Network
		rng    *rand.Rand
		pool   *pool
		dsql   *dsql
		gate   *gate.Policy
		health *health
		read   ReadPath
		txRTs  int
		slow   Slow

		// granted is what each caller waiting at the gate does once it has a slot.
		granted    map[*gate.Waiter]func()
		gateWaited int64 // admissions that waited at the gate, for queued facts
		stats      StoreStats
	}

	// lease is a connection a caller holds, with its gate ticket.
	lease struct {
		c      *conn
		ticket *gate.Ticket
	}

	// StoreStats counts what the plugin did with the store's statements.
	StoreStats struct {
		// Statements counts the statements that ran on a connection, and CutOff those cut
		// off at their deadline.
		Statements, CutOff int
		// Refused counts the gate's refusals, Rejected the callers whose connection DSQL
		// rejected, and AcquireTimeouts the callers whose deadline passed in the pool.
		Refused, Rejected, AcquireTimeouts int
	}

	acquireCounts struct {
		acquired, queued int64
	}
)

func newStore(l *loop, h Host, net Network, slow Slow, p *pool, d *dsql, health *health, timeSource simClock, seed uint64, name string) *store {
	s := &store{
		l:       l,
		net:     net,
		slow:    slow,
		rng:     randoms(seed, name+"/store"),
		pool:    p,
		dsql:    d,
		health:  health,
		read:    h.Read,
		txRTs:   h.History.TxRoundTrips,
		granted: make(map[*gate.Waiter]func()),
	}
	if h.Gate {
		s.gate = gate.NewPolicy(h.Pool.MaxConns, timeSource)
	}
	return s
}

// connect takes a connection for a statement or a transaction, as the plugin's connector
// does. A zero deadline waits for as long as it takes.
func (s *store) connect(kind gate.Kind, deadline time.Time, done func(*lease, connectResult)) {
	if s.gate == nil {
		s.acquire(nil, deadline, done)
		return
	}
	admission := s.gate.Admit(kind, deadline, !deadline.IsZero())
	if admission.Ticket != nil {
		s.acquire(admission.Ticket, deadline, done)
		return
	}
	w := admission.Waiter
	if w == nil {
		s.refuse(done)
		return
	}
	var giveUp *event
	if !deadline.IsZero() {
		giveUp = s.l.at(w.GiveUp(), func() {
			if !s.gate.Withdraw(w) {
				delete(s.granted, w)
				s.refuse(done)
			}
		})
	}
	s.granted[w] = func() {
		giveUp.cancel()
		s.gateWaited++
		s.acquire(w.Ticket(), deadline, done)
	}
}

func (s *store) refuse(done func(*lease, connectResult)) {
	s.stats.Refused++
	s.health.reportFact(s.l.now, persistence.StoreFactThrottled, 1)
	s.l.soon(func() { done(nil, connectRefused) })
}

// acquire takes a connection from the pool for a caller the gate admitted.
func (s *store) acquire(ticket *gate.Ticket, deadline time.Time, done func(*lease, connectResult)) {
	s.pool.acquire(deadline, func(c *conn, result acquireResult) {
		switch result {
		case acquired:
			done(&lease{c: c, ticket: ticket}, connected)
		case acquireRejected:
			s.stats.Rejected++
			s.health.reportFact(s.l.now, persistence.StoreFactThrottled, 1)
			s.releaseTicket(ticket, false)
			done(nil, connectRejected)
		default: // acquireTimedOut
			s.stats.AcquireTimeouts++
			s.releaseTicket(ticket, false)
			done(nil, connectTimedOut)
		}
	})
}

// release gives a connection back to the pool, then its slot to the gate, as the plugin's
// connection does when database/sql closes it.
func (s *store) release(ls *lease) {
	s.pool.release(ls.c)
	s.releaseTicket(ls.ticket, true)
}

func (s *store) releaseTicket(ticket *gate.Ticket, used bool) {
	if ticket == nil {
		return
	}
	if w := ticket.Release(used); w != nil {
		resume := s.granted[w]
		delete(s.granted, w)
		s.l.soon(resume)
	}
}

// run runs a statement of roundTrips round trips, each taking server time at the store, on
// the lease's connection, and reports whether it completed. A statement still running at
// its deadline is cut off: pgx closes the connection and sends a cancel request.
func (s *store) run(ls *lease, roundTrips int, server time.Duration, deadline time.Time, done func(bool)) {
	s.stats.Statements++
	c := ls.c
	d := jittered(s.rng, time.Duration(roundTrips)*(s.net.RoundTrip+server), s.net.Jitter)
	if s.slow.holds(s.l.now) {
		d += time.Duration(roundTrips) * s.slow.Delay
	}
	end := func(completed bool) {
		c.onCut = nil
		done(completed)
	}
	var finish *event
	if !deadline.IsZero() && s.l.now.Add(d).After(deadline) {
		finish = s.l.at(deadline, func() {
			s.stats.CutOff++
			s.dsql.cancelQuery(c)
			end(false)
		})
	} else {
		finish = s.l.after(d, func() { end(true) })
	}
	// DSQL closing the connection fails the statement, and pgx closes its side as it
	// would for a query it cut off.
	c.onCut = func() {
		finish.cancel()
		s.dsql.cancelQuery(c)
		end(false)
	}
}

// statement takes a connection, runs roundTrips on it, and gives it back. keepExhausted
// keeps a refusal's ResourceExhausted, as the plugin's transactions and its pipelined read
// do; the common store's reads and the history append flatten it to Unavailable.
func (s *store) statement(kind gate.Kind, roundTrips int, server time.Duration, deadline time.Time, keepExhausted bool, done func(failure)) {
	s.connect(kind, deadline, func(ls *lease, result connectResult) {
		switch result {
		case connected:
			s.run(ls, roundTrips, server, deadline, func(completed bool) {
				s.release(ls)
				if completed {
					done(ok)
				} else {
					done(unavailable)
				}
			})
		case connectRefused, connectRejected:
			if keepExhausted {
				done(exhausted)
			} else {
				done(unavailable)
			}
		default: // connectTimedOut
			done(unavailable)
		}
	})
}

// readWorkflow is GetWorkflowExecution: the common store's nine queries in turn, each on a
// connection of its own, or the plugin's read, its queries sent as one batch on one
// connection.
func (s *store) readWorkflow(deadline time.Time, done func(failure)) {
	if s.read == PipelinedRead {
		s.statement(gate.Read, 1, s.net.Pipelined, deadline, true, done)
		return
	}
	s.readQuery(0, deadline, done)
}

func (s *store) readQuery(i int, deadline time.Time, done func(failure)) {
	s.statement(gate.Read, 1, s.net.Server, deadline, false, func(f failure) {
		if f != ok || i+1 == readQueries {
			done(f)
			return
		}
		s.readQuery(i+1, deadline, done)
	})
}

// writeWorkflow is UpdateWorkflowExecution: the history append, a statement of its own
// that the plugin marks as a write, then the transaction.
func (s *store) writeWorkflow(deadline time.Time, done func(failure)) {
	s.statement(gate.Write, 1, s.net.Server, deadline, false, func(f failure) {
		if f != ok {
			done(f)
			return
		}
		s.statement(gate.Write, s.txRTs, s.net.Server, deadline, true, done)
	})
}

// reportFacts reports the acquires since the last report every second, as acquired facts,
// and those that waited, at the gate or in the pool, as queued facts.
func (s *store) reportFacts(last acquireCounts) {
	s.l.after(acquireFactsInterval, func() {
		current := acquireCounts{acquired: s.pool.acquires, queued: s.pool.emptyAcquires + s.gateWaited}
		s.health.reportFact(s.l.now, persistence.StoreFactQueued, current.queued-last.queued)
		s.health.reportFact(s.l.now, persistence.StoreFactAcquired, current.acquired-last.acquired)
		s.reportFacts(current)
	})
}
