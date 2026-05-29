# Phase 1: Framework Foundation - KICKOFF

**Date**: 2026-05-28  
**Status**: ✅ Ready to start Phase 1  
**Duration**: Week 1-2  
**Environment**: UAT (`https://dtc-api.lfuat.net/api`)

---

## What We Know (From Phase 0)

✅ API is working  
✅ Data structure is clear  
✅ Date format confirmed (ISO 8601 UTC)  
✅ Row structure understood  
✅ Sample data collected  

**See**: `data_samples/DTC_API_FINDINGS.md`

---

## Phase 1 Objectives

Build the **reusable framework** for N-to-N syncing:

1. **AppConnector base class** - Abstract interface all app connectors implement
2. **DTCConnector implementation** - First concrete connector (DTC-specific logic)
3. **Databricks Delta schema** - Tables for raw snapshots, change logs, push queues
4. **Change detection algorithm** - Snapshot diffing using hashes
5. **Push queue formatting** - App-specific payload preparation

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    PHASE 1 DELIVERABLE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  sync_hub/                                                      │
│  ├── python/connectors/                                         │
│  │   ├── __init__.py                                            │
│  │   ├── base.py              ← AppConnector (abstract)        │
│  │   └── dtc.py               ← DTCConnector (concrete)        │
│  │                                                              │
│  ├── python/client/                                             │
│  │   ├── rest_client.py       ← Generic HTTP client            │
│  │   └── auth.py              ← Auth handling                  │
│  │                                                              │
│  ├── python/diffing/                                            │
│  │   ├── snapshot.py          ← Change detection algorithm     │
│  │   └── hasher.py            ← Row hashing                    │
│  │                                                              │
│  └── notebooks/00_init/                                         │
│      └── init_delta_schema.py ← Create Delta tables            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Task Breakdown

### Task 1: RestClient Wrapper
**File**: `sync_hub/python/client/rest_client.py`

Create a generic REST client with:
- Authentication headers (API key)
- Automatic retry logic (exponential backoff)
- Timeout handling
- Response parsing
- Error handling

```python
class RestClient:
    def __init__(self, base_url, api_key, timeout=30):
        """Initialize REST client with auth"""
        
    @retry(wait=wait_exponential(multiplier=1, min=2, max=60))
    def get(self, endpoint, params=None):
        """GET request with retry"""
        
    @retry(...)
    def post(self, endpoint, data):
        """POST request with retry"""
        
    @retry(...)
    def patch(self, endpoint, data):
        """PATCH request with retry"""
```

**Dependencies**: `requests`, `tenacity` (for retries)

**Estimate**: 2-3 hours

---

### Task 2: AppConnector Base Class
**File**: `sync_hub/python/connectors/base.py`

Define abstract interface that all connectors must implement:

```python
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class AppConnector(ABC):
    """Base class for all app connectors"""
    
    app_name: str  # "dtc", "beproduct", "miro", "xts"
    
    @abstractmethod
    def pull(self, since_timestamp=None) -> DataFrame:
        """
        Fetch all entities from this app.
        
        Returns DataFrame with columns:
        - app_id (String): Record ID in this app
        - entity_type (String): "request", "style", "board", etc.
        - entity_json (String): Full JSON blob
        - fetched_at (Timestamp): When pulled
        - last_modified (Timestamp): Last change in source
        """
        
    @abstractmethod
    def push(self, records: DataFrame, change_type: str) -> DataFrame:
        """
        Push changed records back to this app.
        
        Args:
            records: Rows with change_type="INSERT"/"UPDATE"/"DELETE"
            change_type: Which type of change to push
            
        Returns: Status DataFrame with:
        - app_id (String)
        - push_status (String): "success", "failed", "skipped"
        - error (String): Error message if failed
        """
        
    @abstractmethod
    def get_field_schema(self) -> dict:
        """
        Return field definitions for validation.
        
        Returns: {
            "field_name": {
                "type": "string|number|date|...",
                "writable": bool,
                "required": bool
            }
        }
        """
```

**Estimate**: 1-2 hours

---

### Task 3: DTCConnector Implementation
**File**: `sync_hub/python/connectors/dtc.py`

Implement DTC-specific logic:

```python
from connectors.base import AppConnector
from client.rest_client import RestClient

class DTCConnector(AppConnector):
    app_name = "dtc"
    
    def __init__(self, api_key: str, environment: str = "uat"):
        self.client = RestClient(
            base_url=f"https://dtc-api.lf{environment}.net/api",
            api_key=api_key
        )
        self.workspace_name = "Kontoor"  # Hardcoded for now
        
    def pull(self, since_timestamp=None) -> DataFrame:
        """Fetch all requests from DTC"""
        # 1. Get documents (metadata)
        # 2. List requests
        # 3. For each request: get sheet data
        # 4. Flatten to DataFrame
        
    def push(self, records: DataFrame, change_type: str) -> DataFrame:
        """Push changes to DTC via PATCH"""
        # 1. Filter records by change_type
        # 2. For each row: call PATCH /sheets/{sheetId}/views/{viewId}
        # 3. Collect status
        
    def get_field_schema(self) -> dict:
        """Return DTC field types"""
        return {
            "text_field": {"type": "string", "writable": True},
            "number_field": {"type": "number", "writable": True},
            "date_field": {"type": "date", "writable": True},
            ...
        }
```

**Key**: Parse DTC date format (ISO 8601 UTC), flatten sheet rows into dataframe

**Estimate**: 4-6 hours

---

### Task 4: Databricks Delta Schema
**Notebook**: `sync_hub/notebooks/00_init/init_delta_schema.py`

Create base Delta Lake tables:

```sql
-- Raw snapshots (one per app)
CREATE TABLE IF NOT EXISTS main.sync_hub.raw_dtc_snapshots (
  snapshot_date DATE,
  request_id STRING,
  sheet_id STRING,
  view_id STRING,
  row_index INT,
  entity_json STRING,
  row_hash STRING,
  fetched_at TIMESTAMP,
  user_id STRING,
  user_email STRING
) PARTITIONED BY (snapshot_date);

-- Change log (detected changes)
CREATE TABLE IF NOT EXISTS main.sync_hub.change_log (
  change_id STRING,
  source_app STRING,
  request_id STRING,
  row_index INT,
  change_type STRING,    -- INSERT, UPDATE, DELETE
  detected_at TIMESTAMP,
  push_status STRING,    -- pending, synced, failed
  push_error STRING
);

-- Push queue (per app)
CREATE TABLE IF NOT EXISTS main.sync_hub.push_queue_dtc (
  request_id STRING,
  row_index INT,
  change_type STRING,
  payload STRING,        -- JSON for PATCH body
  push_status STRING
);
```

**Estimate**: 1-2 hours

---

### Task 5: Change Detection Algorithm
**File**: `sync_hub/python/diffing/snapshot.py`

Implement hash-based diffing:

```python
def detect_changes(prev_snapshot: DataFrame, 
                   curr_snapshot: DataFrame, 
                   business_key_cols: list) -> DataFrame:
    """
    Compare two snapshots and classify changes.
    
    Uses row_hash to detect modifications without introspecting fields.
    """
    # 1. Add hash column to each snapshot
    # 2. Join on business_key
    # 3. Classify: INSERT, UPDATE, DELETE, NOCHANGE
    # 4. Filter out NOCHANGE rows
    # 5. Return change_log compatible dataframe
```

**Estimate**: 2-3 hours

---

## Implementation Order

**Week 1 (Days 1-3)**:
1. ✅ RestClient wrapper
2. ✅ AppConnector base class
3. ✅ DTCConnector skeleton

**Week 1-2 (Days 4-7)**:
4. ✅ DTCConnector full implementation (pull + push)
5. ✅ Change detection algorithm
6. ✅ Delta schema setup

**Week 2 (Days 8-10)**:
7. ✅ Testing and validation
8. ✅ Documentation

---

## Success Criteria

- [ ] Can instantiate `DTCConnector("api_key")`
- [ ] `connector.pull()` returns DataFrame with 6+ rows from sample request
- [ ] Row hash correctly identifies same row across snapshots
- [ ] Change detection identifies INSERT/UPDATE/DELETE
- [ ] Delta tables created and queryable
- [ ] Can push a test row via PATCH (read-only for now, just test format)

---

## Known Issues to Resolve

1. **User timezone location** ⚠️
   - Current: empty in user profile
   - Action: Call `/users/{userId}` to check all fields, also try `/workspaces/{id}`
   - Impact: Date conversion logic on push

2. **Field name normalization** 
   - Current: HTML tags in names (`"Field<BR/>Name"`)
   - Action: Strip or replace `<BR/>` with `_` or space
   - Impact: Databricks column names should be clean

3. **Sparse row data**
   - Current: Some rows missing fields
   - Action: Handle null/missing gracefully in DataFrame schema
   - Impact: Use nullable types in schema

---

## Environment & Credentials

**API Credentials** (save to `.env`):
```bash
DTC_API_KEY=49A127E0942071B4BD440DD00386C6B3
DTC_ENVIRONMENT=uat
DTC_WORKSPACE_NAME=Kontoor
```

**Databricks**:
- Workspace: Existing
- Catalog: `main`
- Schema: `sync_hub` (create it)
- Path: `/Workspace/beproduct/sync_hub/`

---

## Next Steps

1. **Ensure .env is updated** with DTC credentials
2. **Create folder structure**:
   ```bash
   mkdir -p sync_hub/python/{connectors,client,diffing}
   mkdir -p sync_hub/notebooks/00_init
   touch sync_hub/python/__init__.py
   ```
3. **Start Task 1**: RestClient wrapper (2-3 hours)
4. **Parallel**: Set up Databricks workspace for schema creation

---

## Resources

- **API Reference**: `data_samples/DTC_API_FINDINGS.md`
- **Sample Data**: `data_samples/dtc_exploration_results.json`
- **Architecture Plan**: `.kilo/plans/1779966530296-shiny-comet.md`

---

## Ready to Start?

Phase 1 is fully scoped. All dependencies understood. Can begin immediately.

**Estimated total time**: 2 weeks (including testing/validation)

**Next action**: Create folder structure and start Task 1 (RestClient)

