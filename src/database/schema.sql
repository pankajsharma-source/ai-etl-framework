-- Analytics Interface Storage Schema
-- Role-based storage for data sources and pipeline history

-- ============================================================================
-- Data Sources Configuration Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS analytics_data_sources (
    id VARCHAR(50) PRIMARY KEY,
    role VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    icon VARCHAR(10),
    description TEXT,
    file_path TEXT,
    config JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(id, role)
);

-- Index for fast role-based queries
CREATE INDEX IF NOT EXISTS idx_data_sources_role ON analytics_data_sources(role);

-- ============================================================================
-- Pipeline History Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS analytics_pipeline_history (
    id SERIAL PRIMARY KEY,
    role VARCHAR(50) NOT NULL,
    source_id VARCHAR(50) NOT NULL,
    source_name VARCHAR(255) NOT NULL,
    pipeline_id UUID,
    status VARCHAR(20) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    duration_seconds FLOAT DEFAULT 0,
    error_message TEXT,
    run_type VARCHAR(50),
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_history_role_time ON analytics_pipeline_history(role, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_history_source ON analytics_pipeline_history(source_id);

-- ============================================================================
-- Update trigger for updated_at
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_data_sources_updated_at
    BEFORE UPDATE ON analytics_data_sources
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
