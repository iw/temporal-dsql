package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/mock"
	"go.uber.org/mock/gomock"
)

// factsDataStoreFactory is a data store factory whose plugin reports store facts.
type factsDataStoreFactory struct {
	persistence.DataStoreFactory
	facts persistence.StoreFacts
}

func (f *factsDataStoreFactory) SetStoreFacts(facts persistence.StoreFacts) { f.facts = facts }

func newFactoryWithHealthSignals(
	t *testing.T,
	healthSignals persistence.HealthSignalAggregator,
) *factsDataStoreFactory {
	dataStoreFactory := &factsDataStoreFactory{DataStoreFactory: mock.NewMockDataStoreFactory(gomock.NewController(t))}
	client.NewFactory(
		dataStoreFactory,
		&config.Persistence{NumHistoryShards: 1},
		nil,
		nil,
		nil,
		nil,
		nil,
		"",
		nil,
		nil,
		healthSignals,
		func() bool { return false },
		func() bool { return false },
	)
	return dataStoreFactory
}

func TestNewFactoryPassesHealthSignalsAsStoreFacts(t *testing.T) {
	dataStoreFactory := newFactoryWithHealthSignals(t, persistence.NoopHealthSignalAggregator)
	require.Equal(t, persistence.NoopHealthSignalAggregator, dataStoreFactory.facts)
}

func TestNewFactoryWithoutHealthSignals(t *testing.T) {
	dataStoreFactory := newFactoryWithHealthSignals(t, nil)
	require.Nil(t, dataStoreFactory.facts)
}
