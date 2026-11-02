package calculator

var (
	_ Calculator          = (*ClusterAwareQuotaCalculator)(nil)
	_ NamespaceCalculator = (*ClusterAwareNamespaceQuotaCalculator)(nil)
	_ ShareProvider       = MemberShare{}
)

type (
	// MemberCounter returns the total number of instances there are for a given service.
	MemberCounter interface {
		AvailableMemberCount() int
	}
	// ShareProvider gives this instance its share of a per cluster quota. The namespace is
	// empty for quotas that aren't specific to a namespace. A provider returns false when it
	// has no share for the quota, and the calculator then uses MemberShare.
	ShareProvider interface {
		Share(namespace string, clusterQuota float64) (float64, bool)
	}
	// MemberShare is the share a calculator uses without a ShareProvider: the per cluster
	// quota divided by the number of instances running the same service.
	MemberShare struct {
		MemberCounter MemberCounter
	}
	// ClusterAwareQuotaCalculator calculates the available quota for the current host based on the per instance and per
	// cluster quota. The quota could represent requests per second, total number of active requests, etc. It works by
	// dividing the per cluster quota by the total number of instances running the same service.
	ClusterAwareQuotaCalculator quotaCalculator[func() int]
	// ClusterAwareNamespaceQuotaCalculator is similar to ClusterAwareQuotaCalculator, but it uses quotas that
	// are specific to a namespace.
	ClusterAwareNamespaceQuotaCalculator quotaCalculator[func(namespace string) int]
	// quotaCalculator is a generic type that we use because the quota functions could be namespace specific or not.
	quotaCalculator[T any] struct {
		MemberCounter MemberCounter
		// ShareProvider, if set, gives the host's share of the per cluster limit in place of
		// MemberCounter's equal share, whenever it has one.
		ShareProvider ShareProvider
		// PerInstanceQuota is a function that returns the per instance limit.
		PerInstanceQuota T
		// GlobalQuota is a function that returns the per cluster limit.
		GlobalQuota T
	}
)

// Share divides clusterQuota by the number of instances that the MemberCounter reports, and
// returns false when there are none.
func (s MemberShare) Share(_ string, clusterQuota float64) (float64, bool) {
	if s.MemberCounter == nil {
		return 0, false
	}
	if clusterSize := s.MemberCounter.AvailableMemberCount(); clusterSize > 0 {
		return clusterQuota / float64(clusterSize), true
	}
	return 0, false
}

// getQuota returns the effective resource limit for a host given the per instance and per
// cluster limits. The "resource" here could be requests per second, total number of active requests, etc. The cluster-wide
// limit is used if and only if it is configured to a value greater than zero and the share provider gives a share, or
// the number of instances that the memberCounter reports is greater than zero. Otherwise, the per-instance limit is used.
func getQuota(shares ShareProvider, memberCounter MemberCounter, namespace string, instanceLimit, clusterLimit int) float64 {
	if clusterLimit > 0 {
		if shares != nil {
			if share, ok := shares.Share(namespace, float64(clusterLimit)); ok {
				return share
			}
		}
		if share, ok := (MemberShare{MemberCounter: memberCounter}).Share(namespace, float64(clusterLimit)); ok {
			return share
		}
	}

	return float64(instanceLimit)
}

func (l ClusterAwareQuotaCalculator) GetQuota() float64 {
	return getQuota(l.ShareProvider, l.MemberCounter, "", l.PerInstanceQuota(), l.GlobalQuota())
}

func (l ClusterAwareNamespaceQuotaCalculator) GetQuota(namespace string) float64 {
	return getQuota(l.ShareProvider, l.MemberCounter, namespace, l.PerInstanceQuota(namespace), l.GlobalQuota(namespace))
}
