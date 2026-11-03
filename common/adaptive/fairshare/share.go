// Package fairshare splits a namespace's rate limit across the cluster between frontends by
// their demand. Without it each frontend takes an equal share of the limit, so a namespace
// whose requests reach one frontend more than the others is held below its limit there while
// the others' shares go unused. Here frontends exchange how many requests each namespace asks
// of them, and each takes a share of the limit that follows its part of the demand. See
// docs/dsql/adaptive-behaviour.md, Proposal 7.
//
// Like the other controllers, share.go is stdlib-only, and Share is a pure function of its
// parameters and the demand.
package fairshare

type (
	// Params are the law's parameters.
	Params struct {
		// MinShare is the share of an equal share that each frontend keeps whatever its
		// demand, so it can take requests that arrive before the next exchange.
		MinShare float64
		// Headroom is the share of its demand that a frontend's share keeps spare, while the
		// namespace's demand fits its limit.
		Headroom float64
	}

	// Demand is what a frontend knows of a namespace's requests, in tokens a second: its own,
	// and the sum of those that the other frontends it has fresh reports from received.
	Demand struct {
		Own, Peers float64
		// Members is how many frontends there are, this one included, and Known how many of
		// the others sent a fresh report.
		Members, Known int
	}

	// Detail is a step as its decision's detail records it, so a trace can replay it: the
	// class of limit and the namespace, the namespace's limit across the cluster, and what
	// Share took.
	Detail struct {
		Class, Namespace string
		Limit            float64
		Params           Params
		Demand           Demand
	}

	// Reason explains a share.
	Reason string
)

const (
	ReasonNoDemand Reason = "no-demand" // an equal share, as without the controller
	ReasonWithin   Reason = "within"    // the demand fits the limit, with headroom to spare
	ReasonOver     Reason = "over"      // the demand passes the limit, so shares follow it
)

// Share is this frontend's share of limit, a namespace's limit across the cluster. Each other
// frontend whose demand it doesn't know keeps an equal share, and the frontends it knows split
// the rest: each keeps MinShare of an equal share, and the remainder follows their demand.
// While the demand with Headroom fits the remainder, each frontend gets its demand with
// Headroom and an even part of what's spare. Otherwise each gets a part in proportion to its
// demand, so a request is as likely to be admitted at one frontend as at another. Without
// demand, each frontend gets an equal share.
//
// When the frontends see the same demand and members, their shares add up to the limit.
func Share(p Params, limit float64, d Demand) (float64, Reason) {
	members := max(1, d.Members)
	equal := limit / float64(members)
	known := min(members, max(0, d.Known)+1) // this frontend and the others it knows
	floor := min(1, max(0, p.MinShare)) * equal
	rest := float64(known) * (equal - floor) // what the frontends it knows split above their floors
	own := max(0, d.Own)
	total := own + max(0, d.Peers)
	headroom := max(0, p.Headroom)
	switch {
	case total <= 0 || limit <= 0:
		return equal, ReasonNoDemand
	case total*(1+headroom) <= rest:
		spare := rest - total*(1+headroom)
		return floor + own*(1+headroom) + spare/float64(known), ReasonWithin
	default:
		return floor + rest*own/total, ReasonOver
	}
}
