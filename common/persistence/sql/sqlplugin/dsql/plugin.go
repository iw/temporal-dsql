package dsql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
	"go.temporal.io/server/common/resolver"
)

const (
	// PluginName is the name of the plugin
	PluginName = "dsql"
)

var defaultDatabaseNames = []string{
	"postgres",  // normal PostgreSQL default DB name
	"defaultdb", // special behavior for Aiven: #1389
}

type plugin struct {
	driver         driver.Driver
	queryConverter sqlplugin.VisibilityQueryConverter
	retryConfig    RetryConfig

	// Token cache for IAM authentication (lazily initialized)
	tokenCacheMu           sync.Mutex
	tokenCache             *TokenCache
	credentialsProvider    aws.CredentialsProvider
	credentialsInitialized bool

	// Connection rate limiter (lazily initialized)
	// Can be either local (ConnectionRateLimiter) or distributed (DistributedRateLimiter)
	rateLimiterOnce sync.Once
	rateLimiter     driver.RateLimiter

	ignoredSettingsOnce sync.Once
	lifetimeWarningOnce sync.Once
}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	persistencesql.RegisterPlugin(PluginName, &plugin{
		driver:         &driver.PGXDriver{},
		queryConverter: &queryConverter{},
		retryConfig:    DefaultRetryConfig(),
	})
}

func (p *plugin) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return p.queryConverter
}

// CreateDB initialize the db object
func (p *plugin) CreateDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (sqlplugin.GenericDB, error) {
	connect := func() (*sqlx.DB, error) {
		if cfg.Connect != nil {
			return cfg.Connect(cfg)
		}
		return p.createDSQLConnection(cfg, r, logger, metricsHandler)
	}

	handle := sqlplugin.NewDatabaseHandle(dbKind, connect, func(error) bool { return false }, logger, metricsHandler, clock.NewRealTimeSource())
	db := newDBWithDependencies(dbKind, cfg.DatabaseName, p.driver, handle, nil, logger, metricsHandler, p.retryConfig)

	return db, nil
}

// createDSQLConnection retains the default database fallback for callers without a name.
func (p *plugin) createDSQLConnection(
	cfg *config.SQL, r resolver.ServiceResolver, logger log.Logger, metricsHandler metrics.Handler,
) (*sqlx.DB, error) {
	p.warnIgnoredSettings(logger)
	if IsDistributedConnLeaseEnabled() {
		return nil, errors.New("DSQL connection lease is not supported with pgxpool yet (issue #33)")
	}
	if cfg.DatabaseName != "" {
		return p.createDSQLConnectionWithAuth(cfg, r, logger, metricsHandler)
	}
	defer func() { cfg.DatabaseName = "" }()
	var errs []error
	for _, databaseName := range defaultDatabaseNames {
		cfg.DatabaseName = databaseName
		db, err := p.createDSQLConnectionWithAuth(cfg, r, logger, metricsHandler)
		if err == nil {
			return db, nil
		}
		errs = append(errs, err)
	}
	return nil, serviceerror.NewUnavailable(fmt.Sprintf("unable to connect to DSQL, tried default DB names: %v, errors: %v", strings.Join(defaultDatabaseNames, ","), errs))
}

func (p *plugin) createDSQLConnectionWithAuth(
	cfg *config.SQL, r resolver.ServiceResolver, logger log.Logger, metricsHandler metrics.Handler,
) (*sqlx.DB, error) {
	// Get region from environment variable (following AWS DSQL sample pattern)
	region := os.Getenv("REGION")
	if region == "" {
		// Fallback to AWS_REGION if REGION is not set
		region = os.Getenv("AWS_REGION")
		if region == "" {
			return nil, errors.New("REGION or AWS_REGION environment variable must be set for DSQL IAM authentication")
		}
	}

	// Get cluster endpoint from environment variable (following AWS DSQL sample pattern)
	clusterEndpoint := os.Getenv("CLUSTER_ENDPOINT")
	if clusterEndpoint == "" {
		// Extract hostname from ConnectAddr as fallback
		clusterEndpoint = cfg.ConnectAddr
		if strings.Contains(clusterEndpoint, ":") {
			clusterEndpoint = strings.Split(clusterEndpoint, ":")[0]
		}
	}

	p.rateLimiterOnce.Do(func() {
		if IsDistributedRateLimiterEnabled() {
			// Use distributed rate limiter backed by DynamoDB
			tableName := GetDistributedRateLimiterTable()
			if tableName == "" {
				logger.Error("Distributed rate limiter enabled but DSQL_DISTRIBUTED_RATE_LIMITER_TABLE not set, falling back to local rate limiter")
				p.rateLimiter = NewConnectionRateLimiter()
				return
			}

			// Create DynamoDB client
			ddbClient, err := p.createDynamoDBClient(context.Background(), region, logger)
			if err != nil {
				logger.Error("Failed to create DynamoDB client for distributed rate limiter, falling back to local rate limiter",
					tag.Error(err))
				p.rateLimiter = NewConnectionRateLimiter()
				return
			}

			// Check if token bucket mode is enabled (recommended)
			if IsTokenBucketEnabled() {
				p.rateLimiter = NewTokenBucketLimiter(ddbClient, tableName, clusterEndpoint, logger)
				logger.Info("DSQL token bucket rate limiter initialized",
					tag.NewStringTag("table", tableName),
					tag.NewStringTag("endpoint", clusterEndpoint),
					tag.NewInt64("rate_per_sec", int64(getEnvInt(TokenBucketRateEnvVar, DefaultTokenBucketRate))),
					tag.NewInt64("capacity", int64(getEnvInt(TokenBucketCapacityEnvVar, DefaultTokenBucketCapacity))))
			} else {
				// Legacy per-second counter mode
				p.rateLimiter = NewDistributedRateLimiter(ddbClient, tableName, clusterEndpoint)
				logger.Info("DSQL distributed rate limiter initialized (per-second counter)",
					tag.NewStringTag("table", tableName),
					tag.NewStringTag("endpoint", clusterEndpoint),
					tag.NewInt64("limit_per_second", int64(getEnvInt(DistributedRateLimiterLimitEnvVar, DefaultDistributedRateLimiterLimit))))
			}
		} else {
			// Use local rate limiter (per-instance)
			p.rateLimiter = NewConnectionRateLimiter()
			logger.Info("DSQL local connection rate limiter initialized",
				tag.NewInt("rate_limit", getEnvInt(ConnectionRateLimitEnvVar, DefaultConnectionRateLimit)),
				tag.NewInt("burst_limit", getEnvInt(ConnectionBurstLimitEnvVar, DefaultConnectionBurstLimit)))
		}
	})

	// Get or initialize token cache
	tokenCache, err := p.getOrInitTokenCache(context.Background(), region, logger, metricsHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize token cache: %w", err)
	}

	tokenDuration := GetConfiguredTokenDuration()

	logger.Info("Creating DSQL connection with IAM authentication",
		tag.NewStringTag("endpoint", clusterEndpoint),
		tag.NewStringTag("region", region),
		tag.NewStringTag("user", adminUser),
		tag.NewDurationTag("token_duration", tokenDuration))

	// Create a token provider function that the driver will call for each new connection
	tokenProvider := func(ctx context.Context) (string, error) {
		token, err := tokenCache.GetToken(ctx, clusterEndpoint, region, adminUser, tokenDuration)
		if err != nil {
			logger.Error("Failed to get fresh DSQL token for connection",
				tag.Error(err),
				tag.NewStringTag("endpoint", clusterEndpoint))
			return "", err
		}
		logger.Debug("Got fresh DSQL token for new connection",
			tag.NewStringTag("endpoint", clusterEndpoint))
		return token, nil
	}

	poolConfig, err := p.poolConfig(cfg, r, tokenProvider, logger)
	if err != nil {
		return nil, err
	}
	db, _, err := openPoolDB(poolConfig, metricsHandler)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), poolPingTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping DSQL: %w", err)
	}
	return db, nil
}

// getOrInitTokenCache returns the token cache, initializing it if necessary.
// Uses double-checked locking for thread safety.
func (p *plugin) getOrInitTokenCache(
	ctx context.Context,
	region string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*TokenCache, error) {
	// Fast path: already initialized
	if p.tokenCache != nil && p.credentialsInitialized {
		return p.tokenCache, nil
	}

	// Slow path: initialize with lock
	p.tokenCacheMu.Lock()
	defer p.tokenCacheMu.Unlock()

	// Double-check after acquiring lock
	if p.tokenCache != nil && p.credentialsInitialized {
		return p.tokenCache, nil
	}

	// Resolve credentials once
	credentialsProvider, err := ResolveCredentialsProvider(ctx, region)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve AWS credentials: %w", err)
	}

	p.credentialsProvider = credentialsProvider
	p.tokenCache = NewTokenCache(credentialsProvider, logger, metricsHandler)
	p.credentialsInitialized = true

	logger.Info("DSQL token cache initialized",
		tag.NewStringTag("region", region))

	return p.tokenCache, nil
}

// createDynamoDBClient creates a DynamoDB client for the distributed rate limiter.
func (p *plugin) createDynamoDBClient(
	ctx context.Context,
	region string,
	logger log.Logger,
) (*dynamodb.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	logger.Info("Created DynamoDB client for distributed rate limiter",
		tag.NewStringTag("region", region))

	return client, nil
}
