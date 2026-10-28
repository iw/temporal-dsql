package persistence_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

func TestInitializeSystemNamespaces(t *testing.T) {
	storeErr := errors.New("store unavailable")
	for _, tc := range []struct {
		name     string
		storeErr error
		wantErr  error
	}{
		{name: "created"},
		{name: "already_exists", storeErr: serviceerror.NewNamespaceAlreadyExists("exists")},
		{name: "store_error", storeErr: storeErr, wantErr: storeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := mock.NewMockMetadataStore(gomock.NewController(t))
			serializer := serialization.NewSerializer()
			store.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request *persistence.InternalCreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
					require.Equal(t, primitives.SystemNamespaceID, request.ID)
					require.Equal(t, primitives.SystemLocalNamespace, request.Name)
					require.False(t, request.IsGlobal)
					namespace, err := serializer.NamespaceDetailFromBlob(request.Namespace)
					require.NoError(t, err)
					require.Equal(t, 7*24*time.Hour, namespace.GetConfig().GetRetention().AsDuration())
					require.Equal(t, int64(0), namespace.FailoverVersion)
					require.Equal(t, int64(-1), namespace.FailoverNotificationVersion)
					require.Equal(t, "test-cluster", namespace.ReplicationConfig.ActiveClusterName)
					return &persistence.CreateNamespaceResponse{ID: request.ID}, tc.storeErr
				})
			manager := persistence.NewMetadataManagerImpl(store, serializer, log.NewNoopLogger(), "test-cluster")
			require.ErrorIs(t, manager.InitializeSystemNamespaces(t.Context(), "test-cluster"), tc.wantErr)
		})
	}
}
