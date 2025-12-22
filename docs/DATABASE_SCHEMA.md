# Database Schema Documentation

## Overview

The AI-ETL Framework uses PostgreSQL for persistent storage with organization-based multi-tenancy. All data is isolated by `org_id` (organization UUID).

## Schema Version: 2.0

**Last Updated**: 2025-12-19

---

## Entity Relationship Diagram

```
┌─────────────────┐       ┌──────────────────┐       ┌─────────────────┐
│  organizations  │       │  org_memberships │       │     users       │
├─────────────────┤       ├──────────────────┤       ├─────────────────┤
│ id (PK, UUID)   │◄──────│ org_id (FK)      │───────│ id (PK, UUID)   │
│ name            │       │ user_id (FK)     │──────►│ email (unique)  │
│ slug (unique)   │       │ role             │       │ name            │
│ industry        │       │ created_at       │       │ password_hash   │
│ created_at      │       └──────────────────┘       │ auth_provider   │
│ updated_at      │                                   │ auth_provider_id│
└────────┬────────┘                                   │ created_at      │
         │                                            │ updated_at      │
         │ org_id                                     └─────────────────┘
         │
    ┌────┴────┬─────────────────┬──────────────────┐
    ▼         ▼                 ▼                  ▼
┌────────────────┐  ┌────────────────────┐  ┌────────────────────┐  ┌─────────────────────┐
│ data_sources   │  │ pipeline_history   │  │ source_insights    │  │ visualizations      │
├────────────────┤  ├────────────────────┤  ├────────────────────┤  ├─────────────────────┤
│ id + org_id    │  │ id (PK, serial)    │  │ id (PK, serial)    │  │ id (PK, serial)     │
│ (composite PK) │  │ org_id (FK)        │  │ source_id          │  │ source_id           │
│ name           │  │ source_id          │  │ org_id (FK)        │  │ org_id (FK)         │
│ icon           │  │ source_name        │  │ source_name        │  │ chart_type          │
│ description    │  │ pipeline_id        │  │ data_summary       │  │ chart_title         │
│ file_path      │  │ status             │  │ insights (JSONB)   │  │ x_column            │
│ config (JSONB) │  │ records_processed  │  │ records_analyzed   │  │ y_column            │
│ created_at     │  │ duration_seconds   │  │ generated_from     │  │ chart_config (JSONB)│
│ updated_at     │  │ error_message      │  │ generated_at       │  │ generated_at        │
└────────────────┘  │ run_type           │  └────────────────────┘  └─────────────────────┘
                    │ timestamp          │
                    │ created_at         │
                    └────────────────────┘
```

---

## User Management Tables

### organizations

Central entity for multi-tenancy. All data is isolated by organization.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PRIMARY KEY | Auto-generated UUID |
| name | VARCHAR(255) | NOT NULL | Display name |
| slug | VARCHAR(100) | UNIQUE, NOT NULL | URL-friendly identifier |
| industry | VARCHAR(100) | - | Industry type for personalization |
| created_at | TIMESTAMP | DEFAULT now() | Creation timestamp |
| updated_at | TIMESTAMP | DEFAULT now() | Last update timestamp |

**Industry Values**:
- `healthcare` - Medical claims, HIPAA considerations
- `finance` - Transactions, compliance rules
- `retail` - Sales, inventory patterns
- `technology` - Product metrics, usage data
- `manufacturing` - Supply chain, quality control
- `other` - Generic defaults

**Indexes**:
- `idx_org_slug` on (slug)
- `idx_org_industry` on (industry)

---

### users

Individual user accounts with support for local and OAuth authentication.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PRIMARY KEY | Auto-generated UUID |
| email | VARCHAR(255) | UNIQUE, NOT NULL | User email address |
| name | VARCHAR(255) | - | Display name |
| password_hash | VARCHAR(255) | - | Hashed password (null for OAuth) |
| auth_provider | VARCHAR(50) | DEFAULT 'local' | 'local', 'google', 'github' |
| auth_provider_id | VARCHAR(255) | - | External provider user ID |
| created_at | TIMESTAMP | DEFAULT now() | Creation timestamp |
| updated_at | TIMESTAMP | DEFAULT now() | Last update timestamp |

**Indexes**:
- `idx_users_email` on (email)

---

### org_memberships

Junction table linking users to organizations with roles.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PRIMARY KEY | Auto-generated UUID |
| user_id | UUID | FK → users.id, NOT NULL | User reference |
| org_id | UUID | FK → organizations.id, NOT NULL | Organization reference |
| role | VARCHAR(50) | DEFAULT 'member' | 'owner', 'admin', 'member' |
| created_at | TIMESTAMP | DEFAULT now() | Creation timestamp |

**Constraints**:
- UNIQUE(user_id, org_id) - User can only be in an org once
- ON DELETE CASCADE for both foreign keys

**Indexes**:
- `idx_membership_user` on (user_id)
- `idx_membership_org` on (org_id)

---

## Analytics Tables

### analytics_data_sources

Data source configurations for the ETL/RAG pipeline.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | VARCHAR(50) | NOT NULL | Source identifier |
| org_id | UUID | FK → organizations.id, NOT NULL | Organization reference |
| name | VARCHAR(255) | NOT NULL | Display name |
| icon | VARCHAR(10) | - | Emoji icon |
| description | TEXT | - | Description |
| file_path | TEXT | - | Path to source file |
| config | JSONB | NOT NULL | Configuration (transforms, destinations) |
| created_at | TIMESTAMP | DEFAULT now() | Creation timestamp |
| updated_at | TIMESTAMP | DEFAULT now() | Last update timestamp |

**Primary Key**: (id, org_id) - Composite

**Indexes**:
- `idx_data_sources_org` on (org_id)

---

### analytics_pipeline_history

History of pipeline executions.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | SERIAL | PRIMARY KEY | Auto-increment ID |
| org_id | UUID | FK → organizations.id, NOT NULL | Organization reference |
| source_id | VARCHAR(50) | NOT NULL | Data source ID |
| source_name | VARCHAR(255) | NOT NULL | Data source name |
| pipeline_id | UUID | - | Pipeline execution ID |
| status | VARCHAR(20) | NOT NULL | 'completed', 'failed', 'running' |
| records_processed | INTEGER | DEFAULT 0 | Number of records |
| duration_seconds | FLOAT | DEFAULT 0 | Execution time |
| error_message | TEXT | - | Error details if failed |
| run_type | VARCHAR(50) | - | 'etl', 'rag', 'etl+rag' |
| timestamp | TIMESTAMP | NOT NULL | Execution timestamp |
| created_at | TIMESTAMP | DEFAULT now() | Record creation time |

**Indexes**:
- `idx_history_org_time` on (org_id, timestamp DESC)
- `idx_history_source` on (source_id)

---

### analytics_source_insights

AI-generated insights for data sources.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | SERIAL | PRIMARY KEY | Auto-increment ID |
| source_id | VARCHAR(50) | NOT NULL | Data source ID |
| org_id | UUID | FK → organizations.id, NOT NULL | Organization reference |
| source_name | VARCHAR(255) | - | Data source name |
| source_icon | VARCHAR(10) | - | Emoji icon |
| data_summary | TEXT | - | 2-3 sentence summary |
| insights | JSONB | - | Array of insight strings |
| records_analyzed | INTEGER | DEFAULT 0 | Number of records analyzed |
| generated_from | VARCHAR(10) | - | 'etl' or 'rag' |
| generated_at | TIMESTAMP | DEFAULT now() | Generation timestamp |

**Constraints**:
- UNIQUE(source_id, org_id)

**Indexes**:
- `idx_insights_org` on (org_id)

---

### analytics_visualizations

Auto-generated visualizations (Plotly charts).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | SERIAL | PRIMARY KEY | Auto-increment ID |
| source_id | VARCHAR(50) | NOT NULL | Data source ID |
| org_id | UUID | FK → organizations.id, NOT NULL | Organization reference |
| chart_type | VARCHAR(50) | NOT NULL | 'bar', 'line', 'pie', etc. |
| chart_title | VARCHAR(255) | - | Chart title |
| x_column | VARCHAR(100) | - | X-axis column name |
| y_column | VARCHAR(100) | - | Y-axis column name |
| chart_config | JSONB | NOT NULL | Full Plotly configuration |
| generated_at | TIMESTAMP | DEFAULT now() | Generation timestamp |

**Constraints**:
- UNIQUE(source_id, org_id, chart_type, x_column, y_column)

**Indexes**:
- `idx_viz_source_org` on (source_id, org_id)

---

## Common Queries

### Get all data sources for an organization
```sql
SELECT * FROM analytics_data_sources
WHERE org_id = 'your-org-uuid'
ORDER BY created_at ASC;
```

### Get user's organizations with roles
```sql
SELECT o.*, m.role as membership_role
FROM organizations o
JOIN org_memberships m ON o.id = m.org_id
WHERE m.user_id = 'user-uuid'
ORDER BY o.name;
```

### Get recent pipeline history
```sql
SELECT * FROM analytics_pipeline_history
WHERE org_id = 'your-org-uuid'
ORDER BY timestamp DESC
LIMIT 50;
```

### Get insights for a source
```sql
SELECT * FROM analytics_source_insights
WHERE org_id = 'your-org-uuid'
  AND source_id = 'source-id';
```

---

## Migration

To migrate from role-based to org-based schema, run:

```bash
docker exec -i postgres psql -U etl_user -d etl < src/database/migrations/001_user_management.sql
```

This will:
1. Drop existing analytics tables
2. Create user management tables
3. Recreate analytics tables with `org_id`
4. Create a demo organization for testing

---

## Triggers

### update_updated_at_column()

Automatically updates `updated_at` timestamp on UPDATE operations.

Applied to:
- organizations
- users
- analytics_data_sources
