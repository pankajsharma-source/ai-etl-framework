-- ============================================================================
-- Analytics Interface Storage Schema v2.0
-- Multi-tenant with Organization-based isolation
-- ============================================================================

-- ============================================================================
-- User Management Tables
-- ============================================================================

-- Organizations Table
-- Central entity for multi-tenancy
CREATE TABLE IF NOT EXISTS organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(100) UNIQUE NOT NULL,  -- URL-friendly identifier (e.g., "acme-corp")
    industry VARCHAR(100),               -- For personalization (healthcare, finance, etc.)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_org_slug ON organizations(slug);
CREATE INDEX IF NOT EXISTS idx_org_industry ON organizations(industry);

-- Users Table
-- Individual user accounts
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    password_hash VARCHAR(255),           -- For local auth (null if OAuth only)
    auth_provider VARCHAR(50) DEFAULT 'local',  -- 'local', 'google', 'github'
    auth_provider_id VARCHAR(255),        -- External auth provider user ID
    is_superuser BOOLEAN DEFAULT false,   -- Platform admin with access to all orgs/users
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_superuser ON users(is_superuser) WHERE is_superuser = true;

-- Organization Memberships (Junction Table)
-- Links users to organizations with roles
CREATE TABLE IF NOT EXISTS org_memberships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL DEFAULT 'member',  -- 'owner', 'admin', 'member'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, org_id)
);

CREATE INDEX IF NOT EXISTS idx_membership_user ON org_memberships(user_id);
CREATE INDEX IF NOT EXISTS idx_membership_org ON org_memberships(org_id);

-- Organization Invites Table
-- Invite codes for adding users to organizations
CREATE TABLE IF NOT EXISTS org_invites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    code VARCHAR(20) UNIQUE NOT NULL,      -- Short invite code (e.g., "ABC123XY")
    created_by UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL DEFAULT 'member',  -- Role assigned on join
    max_uses INTEGER DEFAULT 1,            -- How many times code can be used (null = unlimited)
    use_count INTEGER DEFAULT 0,           -- Current number of uses
    expires_at TIMESTAMP,                  -- Optional expiration (null = never expires)
    is_active BOOLEAN DEFAULT true,        -- Can be deactivated by owner
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_invites_org ON org_invites(org_id);
CREATE INDEX IF NOT EXISTS idx_invites_code ON org_invites(code);

-- ============================================================================
-- Analytics Tables (org_id based isolation)
-- ============================================================================

-- Data Sources Configuration Table
CREATE TABLE IF NOT EXISTS analytics_data_sources (
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

CREATE INDEX IF NOT EXISTS idx_data_sources_org ON analytics_data_sources(org_id);

-- Pipeline History Table
CREATE TABLE IF NOT EXISTS analytics_pipeline_history (
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

CREATE INDEX IF NOT EXISTS idx_history_org_time ON analytics_pipeline_history(org_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_history_source ON analytics_pipeline_history(source_id);

-- AI-Generated Source Insights Table
CREATE TABLE IF NOT EXISTS analytics_source_insights (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(50) NOT NULL,
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    source_name VARCHAR(255),
    source_icon VARCHAR(10),
    data_summary TEXT,
    insights JSONB,  -- Array of insight strings
    records_analyzed INTEGER DEFAULT 0,
    generated_from VARCHAR(10),  -- 'etl' or 'rag' - ETL takes precedence
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, org_id)
);

CREATE INDEX IF NOT EXISTS idx_insights_org ON analytics_source_insights(org_id);

-- Auto-Generated Visualizations Table
CREATE TABLE IF NOT EXISTS analytics_visualizations (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(50) NOT NULL,
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    chart_type VARCHAR(50) NOT NULL,      -- 'bar', 'line', 'pie', 'scatter', 'histogram', 'box', 'heatmap'
    chart_title VARCHAR(255),
    x_column VARCHAR(100),
    y_column VARCHAR(100),
    chart_config JSONB NOT NULL,          -- Full Plotly JSON configuration
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, org_id, chart_type, x_column, y_column)
);

CREATE INDEX IF NOT EXISTS idx_viz_source_org ON analytics_visualizations(source_id, org_id);

-- ============================================================================
-- Triggers
-- ============================================================================

-- Update trigger for updated_at columns
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply to organizations
DROP TRIGGER IF EXISTS update_organizations_updated_at ON organizations;
CREATE TRIGGER update_organizations_updated_at
    BEFORE UPDATE ON organizations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply to users
DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply to data_sources
DROP TRIGGER IF EXISTS update_data_sources_updated_at ON analytics_data_sources;
CREATE TRIGGER update_data_sources_updated_at
    BEFORE UPDATE ON analytics_data_sources
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
