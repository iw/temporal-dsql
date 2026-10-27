-- Migration v1.1: Add current_chasm_executions table for CHASM support
-- Derived from PostgreSQL v1.19 schema
-- CHASM = Component-based Hierarchical Architecture for State Management
-- This table stores current executions for non-workflow archetypes (standalone activities, schedulers)

CREATE TABLE current_chasm_executions(
  shard_id INTEGER NOT NULL,
  namespace_id UUID NOT NULL,           -- DSQL: UUID instead of BYTEA
  business_id VARCHAR(255) NOT NULL,    -- Maps to WorkflowID for workflows
  archetype_id BIGINT NOT NULL,
  --
  run_id UUID NOT NULL,                 -- DSQL: UUID instead of BYTEA
  create_request_id VARCHAR(255) NOT NULL,
  state INTEGER NOT NULL,
  status INTEGER NOT NULL,
  start_version BIGINT NOT NULL DEFAULT 0,
  start_time TIMESTAMP NULL,
  last_write_version BIGINT NOT NULL,
  data BYTEA NULL,
  data_encoding VARCHAR(16) NOT NULL DEFAULT '',
  PRIMARY KEY (shard_id, namespace_id, business_id, archetype_id)
);
