-- convert_ts converts a timestamp in RFC3339 to UTC timestamp without time zone.
-- DSQL has no PL/pgSQL, so it's an SQL function.
CREATE OR REPLACE FUNCTION convert_ts(s VARCHAR) RETURNS TIMESTAMP
  LANGUAGE sql IMMUTABLE RETURNS NULL ON NULL INPUT
  AS $$ SELECT s::timestamptz AT TIME ZONE 'UTC' $$;

CREATE TABLE executions_visibility (
  namespace_id            CHAR(64)      NOT NULL,
  run_id                  CHAR(64)      NOT NULL,
  _version                BIGINT        NOT NULL DEFAULT 0, -- increasing version, used to reject upserts which are out of order
  start_time              TIMESTAMP     NOT NULL,
  execution_time          TIMESTAMP     NOT NULL,
  workflow_id             VARCHAR(255)  NOT NULL,
  workflow_type_name      VARCHAR(255)  NOT NULL,
  status                  INTEGER       NOT NULL,  -- enum WorkflowExecutionStatus {RUNNING, COMPLETED, FAILED, CANCELED, TERMINATED, CONTINUED_AS_NEW, TIMED_OUT}
  close_time              TIMESTAMP     NULL,
  history_length          BIGINT        NULL,
  history_size_bytes      BIGINT        NULL,
  execution_duration      BIGINT        NULL,
  state_transition_count  BIGINT        NULL,
  memo                    BYTEA         NULL,
  encoding                VARCHAR(64)   NOT NULL,
  task_queue              VARCHAR(255)  NOT NULL DEFAULT '',
  search_attributes       JSONB         NULL,
  parent_workflow_id      VARCHAR(255)  NULL,
  parent_run_id           VARCHAR(255)  NULL,
  root_workflow_id        VARCHAR(255)  NOT NULL DEFAULT '',
  root_run_id             VARCHAR(255)  NOT NULL DEFAULT '',

  -- Each search attribute has its own generated column, as in the PostgreSQL schema.
  -- DSQL can't add a generated column to an existing table, so every column is created here.
  -- Text columns hold the words separated by single spaces, with a space at each end,
  -- so a query matches a whole word with LIKE '% word %'. DSQL has no tsvector.

  -- Pre-defined search attributes
  TemporalChangeVersion                 JSONB          GENERATED ALWAYS AS (search_attributes->'TemporalChangeVersion') STORED,
  BinaryChecksums                       JSONB          GENERATED ALWAYS AS (search_attributes->'BinaryChecksums') STORED,
  BatcherUser                           VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'BatcherUser') STORED,
  TemporalScheduledStartTime            TIMESTAMP      GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalScheduledStartTime')) STORED,
  TemporalScheduledById                 VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalScheduledById') STORED,
  TemporalSchedulePaused                BOOLEAN        GENERATED ALWAYS AS ((search_attributes->'TemporalSchedulePaused')::boolean) STORED,
  TemporalNamespaceDivision             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalNamespaceDivision') STORED,
  BuildIds                              JSONB          GENERATED ALWAYS AS (search_attributes->'BuildIds') STORED,
  TemporalPauseInfo                     JSONB          GENERATED ALWAYS AS (search_attributes->'TemporalPauseInfo') STORED,
  TemporalReportedProblems              JSONB          GENERATED ALWAYS AS (search_attributes->'TemporalReportedProblems') STORED,
  TemporalWorkerDeploymentVersion       VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalWorkerDeploymentVersion') STORED,
  TemporalWorkflowVersioningBehavior    VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalWorkflowVersioningBehavior') STORED,
  TemporalWorkerDeployment              VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalWorkerDeployment') STORED,
  TemporalUsedWorkerDeploymentVersions  JSONB          GENERATED ALWAYS AS (search_attributes->'TemporalUsedWorkerDeploymentVersions') STORED,
  TemporalExternalPayloadSizeBytes      BIGINT         GENERATED ALWAYS AS ((search_attributes->'TemporalExternalPayloadSizeBytes')::bigint) STORED,
  TemporalExternalPayloadCount          BIGINT         GENERATED ALWAYS AS ((search_attributes->'TemporalExternalPayloadCount')::bigint) STORED,

  -- Pre-allocated custom search attributes
  Bool01                                BOOLEAN        GENERATED ALWAYS AS ((search_attributes->'Bool01')::boolean) STORED,
  Bool02                                BOOLEAN        GENERATED ALWAYS AS ((search_attributes->'Bool02')::boolean) STORED,
  Bool03                                BOOLEAN        GENERATED ALWAYS AS ((search_attributes->'Bool03')::boolean) STORED,
  Datetime01                            TIMESTAMP      GENERATED ALWAYS AS (convert_ts(search_attributes->>'Datetime01')) STORED,
  Datetime02                            TIMESTAMP      GENERATED ALWAYS AS (convert_ts(search_attributes->>'Datetime02')) STORED,
  Datetime03                            TIMESTAMP      GENERATED ALWAYS AS (convert_ts(search_attributes->>'Datetime03')) STORED,
  Double01                              DECIMAL(20, 5) GENERATED ALWAYS AS ((search_attributes->'Double01')::decimal) STORED,
  Double02                              DECIMAL(20, 5) GENERATED ALWAYS AS ((search_attributes->'Double02')::decimal) STORED,
  Double03                              DECIMAL(20, 5) GENERATED ALWAYS AS ((search_attributes->'Double03')::decimal) STORED,
  Int01                                 BIGINT         GENERATED ALWAYS AS ((search_attributes->'Int01')::bigint) STORED,
  Int02                                 BIGINT         GENERATED ALWAYS AS ((search_attributes->'Int02')::bigint) STORED,
  Int03                                 BIGINT         GENERATED ALWAYS AS ((search_attributes->'Int03')::bigint) STORED,
  Keyword01                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword01') STORED,
  Keyword02                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword02') STORED,
  Keyword03                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword03') STORED,
  Keyword04                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword04') STORED,
  Keyword05                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword05') STORED,
  Keyword06                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword06') STORED,
  Keyword07                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword07') STORED,
  Keyword08                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword08') STORED,
  Keyword09                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword09') STORED,
  Keyword10                             VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'Keyword10') STORED,
  Text01                                TEXT           GENERATED ALWAYS AS (' ' || regexp_replace(search_attributes->>'Text01', '[[:space:]]+', ' ', 'g') || ' ') STORED,
  Text02                                TEXT           GENERATED ALWAYS AS (' ' || regexp_replace(search_attributes->>'Text02', '[[:space:]]+', ' ', 'g') || ' ') STORED,
  Text03                                TEXT           GENERATED ALWAYS AS (' ' || regexp_replace(search_attributes->>'Text03', '[[:space:]]+', ' ', 'g') || ' ') STORED,
  KeywordList01                         JSONB          GENERATED ALWAYS AS (search_attributes->'KeywordList01') STORED,
  KeywordList02                         JSONB          GENERATED ALWAYS AS (search_attributes->'KeywordList02') STORED,
  KeywordList03                         JSONB          GENERATED ALWAYS AS (search_attributes->'KeywordList03') STORED,

  -- Pre-allocated CHASM search attributes
  TemporalBool01                        BOOLEAN        GENERATED ALWAYS AS ((search_attributes->'TemporalBool01')::boolean) STORED,
  TemporalBool02                        BOOLEAN        GENERATED ALWAYS AS ((search_attributes->'TemporalBool02')::boolean) STORED,
  TemporalDatetime01                    TIMESTAMP      GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalDatetime01')) STORED,
  TemporalDatetime02                    TIMESTAMP      GENERATED ALWAYS AS (convert_ts(search_attributes->>'TemporalDatetime02')) STORED,
  TemporalDouble01                      DECIMAL(20, 5) GENERATED ALWAYS AS ((search_attributes->'TemporalDouble01')::decimal) STORED,
  TemporalDouble02                      DECIMAL(20, 5) GENERATED ALWAYS AS ((search_attributes->'TemporalDouble02')::decimal) STORED,
  TemporalInt01                         BIGINT         GENERATED ALWAYS AS ((search_attributes->'TemporalInt01')::bigint) STORED,
  TemporalInt02                         BIGINT         GENERATED ALWAYS AS ((search_attributes->'TemporalInt02')::bigint) STORED,
  TemporalKeyword01                     VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword01') STORED,
  TemporalKeyword02                     VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword02') STORED,
  TemporalKeyword03                     VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword03') STORED,
  TemporalKeyword04                     VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalKeyword04') STORED,
  TemporalLowCardinalityKeyword01       VARCHAR(255)   GENERATED ALWAYS AS (search_attributes->>'TemporalLowCardinalityKeyword01') STORED,
  TemporalKeywordList01                 JSONB          GENERATED ALWAYS AS (search_attributes->'TemporalKeywordList01') STORED,
  TemporalKeywordList02                 JSONB          GENERATED ALWAYS AS (search_attributes->'TemporalKeywordList02') STORED,

  PRIMARY KEY  (namespace_id, run_id)
);

-- DSQL allows 24 indexes on a table, and the primary key counts, so 23 are left.
-- These index the columns that list queries filter on most, plus the first custom
-- attributes of each type, leaving 4 spare. DSQL builds indexes asynchronously, and
-- scans an ascending index backward for the newest-first order.

-- Workflow queries filter on TemporalNamespaceDivision IS NULL; CHASM queries use by_temporal_namespace_division.
CREATE INDEX ASYNC default_idx                              ON executions_visibility (namespace_id, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id) WHERE TemporalNamespaceDivision IS NULL;
CREATE INDEX ASYNC by_execution_time                        ON executions_visibility (namespace_id, execution_time, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_workflow_id                           ON executions_visibility (namespace_id, workflow_id, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_workflow_type                         ON executions_visibility (namespace_id, workflow_type_name, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_status                                ON executions_visibility (namespace_id, status, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_task_queue                            ON executions_visibility (namespace_id, task_queue, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_parent_workflow_id                    ON executions_visibility (namespace_id, parent_workflow_id, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_root_workflow_id                      ON executions_visibility (namespace_id, root_workflow_id, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);

-- Indexes for the predefined search attributes
CREATE INDEX ASYNC by_temporal_namespace_division           ON executions_visibility (namespace_id, TemporalNamespaceDivision, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_temporal_scheduled_by_id              ON executions_visibility (namespace_id, TemporalScheduledById, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_batcher_user                          ON executions_visibility (namespace_id, BatcherUser, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_temporal_worker_deployment            ON executions_visibility (namespace_id, TemporalWorkerDeployment, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_temporal_worker_deployment_version    ON executions_visibility (namespace_id, TemporalWorkerDeploymentVersion, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_temporal_workflow_versioning_behavior ON executions_visibility (namespace_id, TemporalWorkflowVersioningBehavior, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);

-- Indexes for the first pre-allocated custom search attributes
CREATE INDEX ASYNC by_keyword_01                            ON executions_visibility (namespace_id, Keyword01, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_keyword_02                            ON executions_visibility (namespace_id, Keyword02, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_keyword_03                            ON executions_visibility (namespace_id, Keyword03, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_int_01                                ON executions_visibility (namespace_id, Int01, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
CREATE INDEX ASYNC by_datetime_01                           ON executions_visibility (namespace_id, Datetime01, (COALESCE(close_time, '9999-12-31 23:59:59')), start_time, run_id);
