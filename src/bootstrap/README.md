# API Bootstrap Layer

Production-grade API harvesters for chemical synonym collection.

## Overview

The bootstrap layer harvests chemical synonyms from public APIs during the initial database population phase. After harvesting, the system operates locally without requiring further API calls.

## Features

- **Rate-limited API clients** with automatic backoff
- **Persistent caching** to avoid redundant API calls
- **Quality filters** to remove low-value synonyms
- **Comprehensive error handling** for production reliability
- **Progress tracking** with detailed logging

## Supported APIs

### PubChem (NCBI)
- **Rate limit:** 5 requests/second
- **Authentication:** None required
- **Provides:** Synonyms, IUPAC names, molecular formulas, InChI/InChIKey
- **Endpoint:** `https://pubchem.ncbi.nlm.nih.gov/rest/pug`

### Chemical Identifier Resolver (NCI/CACTUS)
- **Rate limit:** 2 requests/second (courtesy)
- **Authentication:** None required  
- **Provides:** Alternative names, SMILES, InChI/InChIKey
- **Endpoint:** `https://cactus.nci.nih.gov/chemical/structure`

### NPRI (Canada)
- **Rate limit:** 2 requests/second
- **Authentication:** None required
- **Provides:** Substance verification (stub implementation)
- **Note:** Requires CSV download implementation

## Quality Filters

Synonyms are filtered using these criteria:

1. **Length**: Drop synonyms >120 characters
2. **Mixture terms**: Drop "mixture", "formulation", "solution", "preparation"
3. **Generic terms**: Drop "standard", "total", "sample", "control", "blank"
4. **Trade names**: Drop if contains ®, ™, ©
5. **Abbreviations**: Validate abbreviations ≤10 chars (alphanumeric + limited punctuation)
6. **ASCII**: Drop non-ASCII unless explicitly allowed
7. **Deduplication**: Case-insensitive deduplication

## Usage

### Basic Harvesting

```python
from src.bootstrap import create_harvesters, filter_synonyms

# Create all harvesters
harvesters = create_harvesters()

# Harvest from PubChem
synonyms = harvesters['pubchem'].harvest_synonyms(
    cas_number="71-43-2",
    chemical_name="Benzene"
)

# Apply quality filters
filtered = filter_synonyms(synonyms, analyte_type="single_substance")

# Close harvesters when done
for harvester in harvesters.values():
    harvester.close()
```

### Using Context Manager

```python
from src.bootstrap import PubChemHarvester

with PubChemHarvester() as harvester:
    # Get synonyms
    synonyms = harvester.harvest_synonyms("71-43-2", "Benzene")
    
    # Get properties
    properties = harvester.get_properties("71-43-2")
    print(properties['MolecularFormula'])  # C6H6
```

### Command-Line Harvesting

```bash
# Harvest all analytes
python scripts/04_harvest_api_synonyms.py

# Harvest with limit (for testing)
python scripts/04_harvest_api_synonyms.py --limit 10

# Harvest from specific source only
python scripts/04_harvest_api_synonyms.py --source pubchem

# Custom database path
python scripts/04_harvest_api_synonyms.py --database data/my_database.db
```

## Architecture

```
src/bootstrap/
├── base_api.py           # Base harvester class with caching & retry logic
├── api_harvesters.py     # Specific API implementations
├── quality_filters.py    # Synonym validation & filtering
└── __init__.py           # Public API
```

### Base API Features

- **Exponential backoff retry**: Automatic retry with increasing delays
- **SQLite-based caching**: Responses cached to `data/raw/api_harvest/{source}/`
- **Rate limiting**: Enforced via `ratelimit` decorator
- **Session pooling**: Persistent HTTP sessions with connection pooling
- **Error logging**: Comprehensive logging with `loguru`

### Quality Filter Functions

```python
from src.bootstrap import (
    filter_synonyms,           # Main filtering function
    clean_synonym_text,        # Remove CAS numbers, parentheticals
    validate_cas_format,       # Validate CAS with check digit
    extract_cas_from_text,     # Find CAS numbers in text
)
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest tests/test_api_harvesters.py -v

# Run specific test class
pytest tests/test_api_harvesters.py::TestPubChemHarvester -v

# Run with coverage
pytest tests/test_api_harvesters.py --cov=src.bootstrap --cov-report=html
```

Demo script:

```bash
python scripts/demo_api_harvesters.py
```

## Performance

### Caching Behavior

- **First run**: Hits APIs, caches responses
- **Subsequent runs**: Uses cache (default: 24-hour expiration)
- **Cache location**: `data/raw/api_harvest/{source}/http_cache.sqlite`

### Rate Limits

| API | Calls/Second | Max Retries | Backoff |
|-----|--------------|-------------|---------|
| PubChem | 5 | 3 | Exponential (1s → 60s) |
| NCI Resolver | 2 | 3 | Exponential (1s → 60s) |
| NPRI | 2 | 3 | Exponential (1s → 60s) |

### Expected Performance

- **~1000 chemicals**: 10-15 minutes (with caching)
- **Quality filter retention**: Typically 60-75%
- **Average synonyms per chemical**: 15-30 (after filtering)

## Error Handling

The harvesters handle:

- **Network timeouts**: Automatic retry with exponential backoff
- **HTTP errors**: 4xx/5xx captured, logged, and recorded in metadata
- **Rate limiting**: 429 responses trigger longer backoff
- **Invalid responses**: JSON parse errors handled gracefully
- **Missing data**: 404 responses logged, no synonyms returned

All errors are recorded in the `api_harvest_metadata` table for audit and debugging.

## Database Schema

### api_harvest_metadata

Tracks all API calls:

```sql
CREATE TABLE api_harvest_metadata (
    id INTEGER PRIMARY KEY,
    api_source VARCHAR(100),
    api_endpoint VARCHAR(500),
    analyte_id INTEGER,
    status_code INTEGER,
    success BOOLEAN,
    synonyms_harvested INTEGER,
    error_message TEXT,
    response_time_ms INTEGER,
    created_at DATETIME
);
```

### synonyms

Stores harvested synonyms:

```sql
CREATE TABLE synonyms (
    id INTEGER PRIMARY KEY,
    analyte_id INTEGER,
    synonym_raw VARCHAR(500),
    synonym_norm VARCHAR(500),
    harvest_source VARCHAR(100),  -- 'pubchem', 'nci', 'npri'
    confidence FLOAT,
    created_at DATETIME
);
```

## Extending

### Adding a New API Harvester

1. Subclass `BaseAPIHarvester`
2. Implement `harvest_synonyms()` and `get_rate_limit()`
3. Add rate-limited request method
4. Register in `create_harvesters()`

Example:

```python
from src.bootstrap.base_api import BaseAPIHarvester
from ratelimit import limits, sleep_and_retry

class MyAPIHarvester(BaseAPIHarvester):
    BASE_URL = "https://api.example.com"
    
    def get_rate_limit(self) -> tuple[int, int]:
        return (10, 1)  # 10 calls per second
    
    @sleep_and_retry
    @limits(calls=10, period=1)
    def _rate_limited_request(self, url: str, **kwargs):
        return self._make_request(url, **kwargs)
    
    def harvest_synonyms(self, cas_number: str, chemical_name: str):
        url = f"{self.BASE_URL}/synonyms/{cas_number}"
        response = self._rate_limited_request(url)
        data = self._parse_json_response(response)
        return data.get('synonyms', [])
```

### Adding a Quality Filter

Add filter logic to `quality_filters.py`:

```python
def my_custom_filter(synonyms: List[str]) -> List[str]:
    """Filter based on custom criteria."""
    return [s for s in synonyms if meets_criteria(s)]
```

Then use in `filter_synonyms()` function.

## Maintenance

### Clear Cache

```python
from src.bootstrap import PubChemHarvester

harvester = PubChemHarvester()
harvester.clear_cache()
harvester.close()
```

### Re-harvest Specific Chemicals

```sql
-- Delete existing synonyms
DELETE FROM synonyms 
WHERE analyte_id IN (
    SELECT id FROM analytes WHERE cas_number = '71-43-2'
)
AND harvest_source = 'pubchem';

-- Re-run harvest
python scripts/04_harvest_api_synonyms.py --source pubchem --limit 1
```

### Monitor Harvest Quality

```sql
-- Harvest success rate by source
SELECT 
    api_source,
    COUNT(*) as total_calls,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful,
    AVG(synonyms_harvested) as avg_synonyms,
    AVG(response_time_ms) as avg_response_ms
FROM api_harvest_metadata
GROUP BY api_source;

-- Synonym counts by source
SELECT 
    harvest_source,
    COUNT(*) as total_synonyms,
    COUNT(DISTINCT analyte_id) as chemicals_covered
FROM synonyms
GROUP BY harvest_source;
```

## Troubleshooting

### Network Issues

```
APIError: Request timeout for https://pubchem...
```

**Solution**: Check internet connection, increase timeout in `BaseAPIHarvester.__init__()`

### Rate Limiting

```
RateLimitExceeded: Rate limit exceeded: 429
```

**Solution**: Reduce rate limit in harvester, wait before retrying

### No Synonyms Found

**Possible causes:**
- Chemical not in database (404 response - normal)
- Invalid CAS number
- API service down

**Check logs**: `logs/harvest_api_synonyms_*.log`

## License

Internal tool - Ontario Regulation 153/04 specific implementation.
