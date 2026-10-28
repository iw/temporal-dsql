package dsql

import (
	"context"
	"fmt"
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
	rateLimiterOnce        sync.Once
	rateLimiter            driver.RateLimiter
	rateLimiterSettings    settings
	rateLimiterWarningOnce sync.Once

	ignoredSettingsOnce sync.Once
	lifetimeWarningOnce sync.Once
	passwordWarningOnce sync.Once
	sslModeWarningOnce  sync.Once
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
	var resolved settings
	if cfg.Connect == nil {
		p.warnIgnoredSettings(logger)
		if err := removedConnectionLeaseError(); err != nil {
			return nil, err
		}
		var err error
		resolved, err = resolveSettings(cfg, logger)
		if err != nil {
			return nil, err
		}
	}
	connect := func() (*sqlx.DB, error) {
		if cfg.Connect != nil {
			return cfg.Connect(cfg)
		}
		return p.connectWithSettings(cfg, resolved, r, logger, metricsHandler)
	}

	handle := sqlplugin.NewDatabaseHandle(dbKind, connect, func(error) bool { return false }, logger, metricsHandler, clock.NewRealTimeSource())
	db := newDBWithDependencies(dbKind, cfg.DatabaseName, p.driver, handle, nil, logger, metricsHandler, p.retryConfig)

	return db, nil
}

// connectWithSettings retains the default database fallback for callers without a name.
func (p *plugin) connectWithSettings(
	cfg *config.SQL, resolved settings, r resolver.ServiceResolver, logger log.Logger, metricsHandler metrics.Handler,
) (*sqlx.DB, error) {
	if cfg.DatabaseName != "" {
		return p.createDSQLConnectionWithAuth(cfg, resolved, r, logger, metricsHandler)
	}
	copyConfig := *cfg
	cfg = &copyConfig
	var errs []error
	for _, databaseName := range defaultDatabaseNames {
		cfg.DatabaseName = databaseName
		db, err := p.createDSQLConnectionWithAuth(cfg, resolved, r, logger, metricsHandler)
		if err == nil {
			return db, nil
		}
		errs = append(errs, err)
	}
	return nil, serviceerror.NewUnavailable(fmt.Sprintf("unable to connect to DSQL, tried default DB names: %v, errors: %v", strings.Join(defaultDatabaseNames, ","), errs))
}

func (p *plugin) createDSQLConnectionWithAuth(
	cfg *config.SQL, resolved settings, r resolver.ServiceResolver, logger log.Logger, metricsHandler metrics.Handler,
) (*sqlx.DB, error) {
	p.initRateLimiter(resolved, logger, metricsHandler)

	// Get or initialize token cache
	tokenCache, err := p.tokenCacheForSettings(context.Background(), resolved, logger, metricsHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize token cache: %w", err)
	}

	tokenProvider := p.tokenProvider(cfg, tokenCache, resolved, logger)

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

func dsqlUser(cfg *config.SQL) string {
	if cfg.User == "" {
		return adminUser
	}
	return cfg.User
}

func (p *plugin) tokenProvider(cfg *config.SQL, tokenCache *TokenCache, resolved settings, logger log.Logger) driver.TokenProvider {
	user := dsqlUser(cfg)
	tokenDuration := resolved.TokenDuration
	clusterEndpoint, region := resolved.ClusterEndpoint, resolved.Region

	logger.Info("Creating DSQL connection with IAM authentication",
		tag.NewStringTag("endpoint", clusterEndpoint),
		tag.NewStringTag("region", region),
		tag.NewStringTag("user", user),
		tag.NewDurationTag("token_duration", tokenDuration))

	// Create a token provider function that the driver will call for each new connection
	return func(ctx context.Context) (string, error) {
		token, err := tokenCache.GetToken(ctx, clusterEndpoint, region, user, tokenDuration)
		if err != nil {
			logger.Error("Failed to get fresh DSQL token for connection",
				tag.Error(err),
				tag.NewStringTag("endpoint", clusterEndpoint),
				tag.NewStringTag("user", user))
			return "", err
		}
		logger.Debug("Got fresh DSQL token for new connection",
			tag.NewStringTag("endpoint", clusterEndpoint),
			tag.NewStringTag("user", user))
		return token, nil
	}
}

// tokenCacheForSettings returns the shared cache, keyed by endpoint, region, user and duration.
func (p *plugin) tokenCacheForSettings(
	ctx context.Context,
	resolved settings,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*TokenCache, error) {
	p.tokenCacheMu.Lock()
	defer p.tokenCacheMu.Unlock()

	if p.tokenCache != nil && p.credentialsInitialized {
		return p.tokenCache, nil
	}

	// Resolve credentials once
	credentialsProvider, err := ResolveCredentialsProvider(ctx, resolved.Region)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve AWS credentials: %w", err)
	}

	p.credentialsProvider = credentialsProvider
	p.tokenCache = NewTokenCache(credentialsProvider, logger, metricsHandler)
	p.credentialsInitialized = true

	logger.Info("DSQL token cache initialized",
		tag.NewStringTag("region", resolved.Region))

	return p.tokenCache, nil
}

// createDynamoDBClient creates a DynamoDB client for the distributed rate limiter.
func (p *plugin) createDynamoDBClient(
	ctx context.Context,
	resolved settings,
	logger log.Logger,
) (*dynamodb.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(resolved.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	logger.Info("Created DynamoDB client for distributed rate limiter",
		tag.NewStringTag("region", resolved.Region))

	return client, nil
}

func (p *plugin) initRateLimiter(resolved settings, logger log.Logger, handler metrics.Handler) {
	p.rateLimiterOnce.Do(func() {
		p.rateLimiterSettings = resolved.effectiveLimiterSettings()
		p.rateLimiter = newConnectionRateLimiter(resolved)
		if resolved.Limiter.Mode == localMode {
			logger.Info("DSQL local connection rate limiter initialized", tag.NewStringTag("settings", resolved.limiterDescription()))
			return
		}
		client, err := p.createDynamoDBClient(context.Background(), resolved, logger)
		if err != nil {
			logger.Error("Failed to create DynamoDB client for distributed rate limiter, falling back to local rate limiter", tag.Error(err))
			return
		}
		var distributed driver.RateLimiter
		if resolved.Limiter.Mode == tokenBucketMode {
			distributed = newTokenBucketLimiter(client, resolved)
		} else {
			distributed = newDistributedRateLimiter(client, resolved)
		}
		p.rateLimiter = newFailOpenRateLimiterWithSettings(distributed, resolved, logger, handler)
		logger.Info("DSQL distributed connection rate limiter initialized", tag.NewStringTag("settings", resolved.limiterDescription()))
	})
	if p.rateLimiterSettings != resolved.effectiveLimiterSettings() {
		p.rateLimiterWarningOnce.Do(func() {
			active := p.rateLimiterSettings
			if _, local := p.rateLimiter.(*ConnectionRateLimiter); local {
				active.Limiter.Mode = localMode
				active = active.effectiveLimiterSettings()
			}
			logger.Warn("DSQL process-wide rate limiter keeps the first pool's settings; later pool settings differ",
				tag.NewStringTag("settings_in_use", active.limiterDescription()),
				tag.NewStringTag("requested_settings", resolved.effectiveLimiterSettings().limiterDescription()))
		})
	}
}
