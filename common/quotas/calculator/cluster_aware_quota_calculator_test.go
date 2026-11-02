package calculator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/quotas/quotastest"
)

type quotaCalculatorTestCase struct {
	name          string
	memberCounter MemberCounter
	instanceLimit int
	clusterLimit  int
	expected      float64
}

var quotaCalculatorTestCases = []quotaCalculatorTestCase{
	{
		name:          "both limits set",
		memberCounter: quotastest.NewFakeMemberCounter(4),
		instanceLimit: 10,
		clusterLimit:  20,
		expected:      5.0,
	},
	{
		name:          "no per cluster limit",
		memberCounter: quotastest.NewFakeMemberCounter(4),
		instanceLimit: 10,
		clusterLimit:  0,
		expected:      10.0,
	},
	{
		name:          "no hosts",
		memberCounter: quotastest.NewFakeMemberCounter(0),
		instanceLimit: 10,
		clusterLimit:  20,
		expected:      10.0,
	},
	{
		name:          "nil member counter",
		memberCounter: nil,
		instanceLimit: 10,
		clusterLimit:  20,
		expected:      10.0,
	},
}

func TestClusterAwareQuotaCalculator_GetQuota(t *testing.T) {
	t.Parallel()

	for _, tc := range quotaCalculatorTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, ClusterAwareQuotaCalculator{
				MemberCounter:    tc.memberCounter,
				PerInstanceQuota: dynamicconfig.GetIntPropertyFn(tc.instanceLimit),
				GlobalQuota:      dynamicconfig.GetIntPropertyFn(tc.clusterLimit),
			}.GetQuota())

		})
	}
}

type perNamespaceQuota struct {
	t     *testing.T
	quota int
}

func (l perNamespaceQuota) getQuota(ns string) int {
	if ns != "test-namespace" {
		l.t.Errorf("unexpected namespace: %s", ns)
	}
	return l.quota
}

func TestClusterAwareNamespaceSpecificQuotaCalculator_GetQuota(t *testing.T) {
	t.Parallel()

	for _, tc := range quotaCalculatorTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			instanceLimit := perNamespaceQuota{t: t, quota: tc.instanceLimit}

			clusterLimit := perNamespaceQuota{t: t, quota: tc.clusterLimit}

			assert.Equal(t, tc.expected, ClusterAwareNamespaceQuotaCalculator{
				MemberCounter:    tc.memberCounter,
				PerInstanceQuota: instanceLimit.getQuota,
				GlobalQuota:      clusterLimit.getQuota,
			}.GetQuota("test-namespace"))
		})
	}
}

// fixedShare is a ShareProvider with one share for one namespace.
type fixedShare struct {
	namespace string
	share     float64
}

func (f fixedShare) Share(namespace string, _ float64) (float64, bool) {
	return f.share, namespace == f.namespace
}

func TestQuotaCalculatorShareProvider(t *testing.T) {
	t.Parallel()

	calc := ClusterAwareNamespaceQuotaCalculator{
		MemberCounter:    quotastest.NewFakeMemberCounter(4),
		ShareProvider:    fixedShare{namespace: "test-namespace", share: 15},
		PerInstanceQuota: func(string) int { return 10 },
		GlobalQuota:      func(string) int { return 20 },
	}
	require.InDelta(t, 15.0, calc.GetQuota("test-namespace"), 1e-9, "the provider's share")
	require.InDelta(t, 5.0, calc.GetQuota("other"), 1e-9, "an equal share when the provider has none")

	calc.GlobalQuota = func(string) int { return 0 }
	require.InDelta(t, 10.0, calc.GetQuota("test-namespace"), 1e-9, "the per instance limit without a per cluster limit")

	hostCalc := ClusterAwareQuotaCalculator{
		MemberCounter:    quotastest.NewFakeMemberCounter(0),
		ShareProvider:    fixedShare{share: 7},
		PerInstanceQuota: dynamicconfig.GetIntPropertyFn(10),
		GlobalQuota:      dynamicconfig.GetIntPropertyFn(20),
	}
	require.InDelta(t, 7.0, hostCalc.GetQuota(), 1e-9, "a host quota's share, with no namespace")
}
