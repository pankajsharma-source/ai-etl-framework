-- ============================================================================
-- Migration 001: User Management Schema
-- ============================================================================
-- This migration:
-- 1. Drops existing analytics tables (fresh start)
-- 2. Creates user management tables (organizations, users, org_memberships)
-- 3. Recreates analytics tables with org_id instead of role
-- ============================================================================

-- ============================================================================
-- STEP 1: Drop existing tables (fresh start)
-- ============================================================================
DROP TABLE IF EXISTS analytics_visualizations CASCADE;
DROP TABLE IF EXISTS analytics_source_insights CASCADE;
DROP TABLE IF EXISTS analytics_pipeline_history CASCADE;
DROP TABLE IF EXISTS analytics_data_sources CASCADE;

-- Drop old trigger function if exists
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

-- ============================================================================
-- STEP 2: Create User Management Tables
-- ============================================================================

-- Organizations Table
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(100) UNIQUE NOT NULL,
    industry VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_org_slug ON organizations(slug);
CREATE INDEX idx_org_industry ON organizations(industry);

-- Users Table
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    password_hash VARCHAR(255),
    auth_provider VARCHAR(50) DEFAULT 'local',
    auth_provider_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);

-- Organization Memberships (Junction Table)
CREATE TABLE org_memberships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL DEFAULT 'member',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, org_id)
);

CREATE INDEX idx_membership_user ON org_memberships(user_id);
CREATE INDEX idx_membership_org ON org_memberships(org_id);

-- ============================================================================
-- STEP 3: Recreate Analytics Tables with org_id
-- ============================================================================

-- Data Sources Configuration Table
CREATE TABLE analytics_data_sources (
    id VARCHAR(50) NOT NULL,
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    icon VARCHAR(10),
    description TEXT,
    file_path TEXT,
    config JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, org_id)
);

CREATE INDEX idx_data_sources_org ON analytics_data_sources(org_id);

-- Pipeline History Table
CREATE TABLE analytics_pipeline_history (
    id SERIAL PRIMARY KEY,
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
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

CREATE INDEX idx_history_org_time ON analytics_pipeline_history(org_id, timestamp DESC);
CREATE INDEX idx_history_source ON analytics_pipeline_history(source_id);

-- AI-Generated Source Insights Table
CREATE TABLE analytics_source_insights (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(50) NOT NULL,
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    source_name VARCHAR(255),
    source_icon VARCHAR(10),
    data_summary TEXT,
    insights JSONB,
    records_analyzed INTEGER DEFAULT 0,
    generated_from VARCHAR(10),
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, org_id)
);

CREATE INDEX idx_insights_org ON analytics_source_insights(org_id);

-- Auto-Generated Visualizations Table
CREATE TABLE analytics_visualizations (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(50) NOT NULL,
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    chart_type VARCHAR(50) NOT NULL,
    chart_title VARCHAR(255),
    x_column VARCHAR(100),
    y_column VARCHAR(100),
    chart_config JSONB NOT NULL,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, org_id, chart_type, x_column, y_column)
);

CREATE INDEX idx_viz_source_org ON analytics_visualizations(source_id, org_id);

-- ============================================================================
-- STEP 4: Create updated_at trigger function
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to tables with updated_at
CREATE TRIGGER update_organizations_updated_at
    BEFORE UPDATE ON organizations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_data_sources_updated_at
    BEFORE UPDATE ON analytics_data_sources
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- STEP 5: Create demo organization for testing
-- ============================================================================
INSERT INTO organizations (name, slug, industry)
VALUES ('Demo Organization', 'demo', 'healthcare');

-- ============================================================================
-- Migration Complete
-- ============================================================================
-- Next steps:
-- 1. Update analytics_db.py to use org_id instead of role
-- 2. Update API endpoints to pass org_id
-- 3. Re-run ETL pipelines to populate fresh data
-- ============================================================================
