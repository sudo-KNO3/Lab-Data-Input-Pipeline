# Chemical Database API Access Research Report
**Date:** February 12, 2026  
**Purpose:** Identify API access methods, authentication requirements, and implementation strategies for chemical databases

---

## Executive Summary

This research identifies 6 major chemical databases for implementation in a chemical matching MVP. **Key Finding:** Most databases require NO API keys for basic access, making MVP development straightforward and cost-free.

### Priority Recommendation for MVP:
1. **Start with PubChem** (no auth, unlimited free API)
2. **Add NCI Resolver** (no auth, simple URL-based API)
3. **Consider EPA CompTox** (appears open, needs verification)
4. **Defer ChemSpider and CAS** (require registration/limited access)

---

## 1. PubChem API ⭐ **HIGHEST PRIORITY**

### Authentication: **❌ NO** (Completely Open)

**Official Documentation:**
- Tutorial: https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest-tutorial
- Specification: https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest  
- API Base URL: `https://pubchem.ncbi.nlm.nih.gov/rest/pug/`

### Access Requirements:
- **No API key needed**
- **No registration required**
- **Completely free and open**
- HTTPS only (as of recent policy)

### Rate Limits:
- **5 requests per second** maximum (self-enforced)
- No hard limits enforced by API keys
- Dynamic request throttling for excessive use
- May return HTTP 503 if server is busy
- **Note:** Over 165 million substances available

### Example API Calls:

**Get compound by CID (aspirin = 2244):**
```
https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/2244/property/MolecularFormula,MolecularWeight,InChIKey/JSON
```

**Search by name:**
```
https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/aspirin/cids/JSON
```

**Get properties by CAS number:**
```
https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/50-00-0/property/MolecularFormula,IUPACName/JSON
```

**Search by InChI Key:**
```
https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/inchikey/BSYNRYMUTXBXSQ-UHFFFAOYSA-N/JSON
```

### Python Example (No Authentication):
```python
import requests
import time

def get_pubchem_compound(cid):
    """Get compound data from PubChem - no auth needed"""
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cid}/JSON"
    response = requests.get(url)
    time.sleep(0.2)  # Respect 5 requests/second limit
    return response.json()

# Example: Get aspirin
aspirin = get_pubchem_compound(2244)
print(aspirin['PC_Compounds'][0]['props'])
```

### GitHub Examples:
- **PubChemPy Library:** https://github.com/mcs07/PubChemPy
  - Wrapper library that handles PubChem API
  - No authentication code needed
  - Automatic rate limiting built-in
  
**Installation:**
```bash
pip install pubchempy
```

**Usage:**
```python
import pubchempy as pcp

# Get compound by CID
compound = pcp.Compound.from_cid(2244)
print(compound.molecular_formula)  # C9H8O4
print(compound.iupac_name)  # 2-acetyloxybenzoic acid

# Search by name
compounds = pcp.get_compounds('glucose', 'name')
for c in compounds:
    print(c.cid, c.molecular_formula)
```

### Bulk Download Alternative:
- FTP site: ftp://ftp.ncbi.nlm.nih.gov/pubchem/
- Download entire database for offline use
- SDF, CSV, and other formats available

### Advantages for MVP:
✅ No authentication complexity  
✅ Comprehensive data (>100M compounds)  
✅ Mature, stable API  
✅ Excellent documentation  
✅ Free Python library available  
✅ Government-funded (reliable)

---

## 2. NCI Chemical Identifier Resolver ⭐ **SECOND PRIORITY**

### Authentication: **❌ NO** (Completely Open)

**Official Service:**
- URL: https://cactus.nci.nih.gov/chemical/structure
- Documentation: https://cactus.nci.nih.gov/

### Access Requirements:
- **No API key needed**
- **No registration required**
- **Completely free**
- Simple URL-based API

### Rate Limits:
- **No explicit rate limits documented**
- Reasonable use expected
- May be slower than PubChem for some queries

### API URL Pattern:
```
https://cactus.nci.nih.gov/chemical/structure/{identifier}/{representation}
```

### Example API Calls:

**Name to InChI Key:**
```
https://cactus.nci.nih.gov/chemical/structure/aspirin/stdinchikey
```

**CAS to SMILES:**
```
https://cactus.nci.nih.gov/chemical/structure/50-00-0/smiles
```

**InChI to molecular formula:**
```
https://cactus.nci.nih.gov/chemical/structure/InChI=1S/C2H6O/c1-2-3/h3H,2H2,1H3/formula
```

**SMILES to image:**
```
https://cactus.nci.nih.gov/chemical/structure/CC(=O)Oc1ccccc1C(=O)O/image
```

### Supported Identifiers:
- Chemical names
- CAS numbers
- SMILES
- InChI / InChI Key
- SDF format

### Output Representations:
- `stdinchikey` - Standard InChI Key
- `stdinchi` - Standard InChI
- `smiles` - SMILES notation
- `formula` - Molecular formula
- `names` - All synonyms
- `image` - 2D structure image
- `sdf` - SDF file format
- ...and many more

### Python Example (No Authentication):
```python
import requests

def nci_resolve(identifier, output_format='stdinchikey'):
    """Resolve chemical identifier using NCI service"""
    url = f"https://cactus.nci.nih.gov/chemical/structure/{identifier}/{output_format}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text.strip()
    else:
        return None

# Examples
inchi_key = nci_resolve('aspirin', 'stdinchikey')
smiles = nci_resolve('50-00-0', 'smiles')  # CAS to SMILES
formula = nci_resolve('BSYNRYMUTXBXSQ-UHFFFAOYSA-N', 'formula')
```

### Advantages for MVP:
✅ No authentication needed  
✅ Simple URL-based API  
✅ Excellent for identifier conversion  
✅ Good synonym resolution  
✅ Government-funded (reliable)

### Limitations:
⚠ Smaller database than PubChem  
⚠ May not have all regulatory chemicals  
⚠ Slower response for some queries

---

## 3. EPA CompTox Chemicals Dashboard

### Authentication: **⚠ UNCLEAR** (Likely Open, Needs Verification)

**Official Resources:**
- Dashboard: https://comptox.epa.gov/dashboard
- Documentation: https://www.epa.gov/chemical-research/chemistry-dashboard
- GitHub (httk R package): https://github.com/USEPA/CompTox-ExpoCast-httk

### Access Status:
- **Web interface is completely open** (no login required)
- **API access unclear** - documentation not easily accessible
- **Bulk downloads available** on website
- **R package (httk) available** - no authentication shown in code

### Database Contents:
- Over **1 million chemicals**
- 300+ chemical lists
- Environmental fate data
- Toxicity data
- High-throughput screening data
- ECOTOX database integration

### Possible Access Methods:

**1. Web Scraping (Last Resort):**
- Search: `https://comptox.epa.gov/dashboard/chemical/details/{DTXSID}`
- Not recommended (terms of service issues)

**2. Bulk Download:**
- Available on website for registered lists
- CSV/SDF formats
- Manual download required

**3. API Investigation Needed:**
- Check if `/api/` endpoint exists
- Look for SWAGGER/OpenAPI documentation
- Contact EPA directly for API access

### R Package Example (httk):
```r
# Install EPA's httk package
install.packages("httk")
library(httk)

# Get chemical info (no API key needed in examples)
get_cheminfo()
chem.cas = "80-05-7"
```

The R package shows **no authentication code**, suggesting data may be embedded or using open endpoints.

### Recommendation for MVP:
⏸ **Defer until API access confirmed**  
- Try contacting EPA for API documentation
- Consider using bulk downloads instead
- Use PubChem/NCI first, add CompTox later if needed

---

## 4. CAS Common Chemistry API

### Authentication: **✅ YES** (Registration Required, Free)

**Official Resources:**
- Website: https://commonchemistry.cas.org/
- API Registration: https://www.cas.org/services/commonchemistry-api
- License: Creative Commons CC BY-NC 4.0 (Non-Commercial)

### Access Requirements:
- **Registration required** via form
- **Free for non-commercial use**
- API key provided after registration
- Must accept CC BY-NC 4.0 license

### Database Contents:
- **Nearly 500,000 chemical substances**
- CAS REGISTRY® data
- Common chemicals
- Regulated chemicals
- High school/undergraduate chemistry compounds
- **Limited compared to full CAS REGISTRY (165M+ substances)**

### Registration Process:
1. Go to: https://www.cas.org/services/commonchemistry-api
2. Fill out request form with:
   - Name
   - Email
   - Organization
   - Use case description
3. Agree to terms (non-commercial use)
4. Receive API key via email

### Rate Limits (Estimated):
- Not publicly documented
- Likely generous for non-commercial use
- Follow API documentation after registration

### Important Limitations:
⚠ **Non-commercial license only**  
⚠ Subset of full CAS REGISTRY  
⚠ Requires registration approval  
⚠ May take time to receive key

### Recommendation for MVP:
⏸ **Register but don't prioritize**  
- Register now to get key for future  
- Use PubChem/NCI for MVP instead  
- Add CAS Common Chemistry later if needed  
- Full commercial CAS Registry requires paid subscription

---

## 5. ChemSpider API

### Authentication: **✅ YES** (RSC Registration Required, Free)

**Official Resources:**
- Developer Portal: https://developer.rsc.org/
- Main Site: https://www.chemspider.com/
- Database: 120+ million unique compounds

### Access Requirements:
- **Royal Society of Chemistry (RSC) account required**
- **Free registration**
- API key provided after login
- Free tier available with rate limits

### Registration Process:
1. Create RSC account at: https://developer.rsc.org/
2. Login to developer portal
3. Register application
4. Generate API key
5. Review API documentation

### Packages & Plans (from website):
- **Free Tier** available
- Rate limitations apply
- Commercial plans for higher usage
- Specific limits not public (need to register to see)

### API Capabilities:
- Search chemical structures
- Name to structure conversion
- Search by molecular formula
- Search by molecular mass
- Access chemical properties
- Structure similarity search

### Example (After Registration):
```python
import requests

API_KEY = "your_rsc_api_key_here"
headers = {"apikey": API_KEY}

# Search by name
url = "https://api.rsc.org/compounds/v1/filter/name"
params = {"name": "aspirin"}
response = requests.get(url, headers=headers, params=params)
```

Note: Exact API URL structure available after registration.

### Recommendation for MVP:
⏸ **Register but secondary priority**  
- Register to get free API key  
- Test rate limits  
- Use PubChem first (no registration)  
- Add ChemSpider if additional coverage needed  

### Advantages:
- Large database (120M compounds)
- RSC-curated data
- Free tier available

### Disadvantages:
- Requires registration
- Rate limits on free tier
- Documentation behind login

---

## 6. NPRI (Canada - National Pollutant Release Inventory)

### Authentication: **❌ NO** (Open Government Data)

**Official Resources:**
- Open Data Portal: https://open.canada.ca/data/en/dataset/1fb7d8d4-7713-4ec6-b957-4a882a84fed3
- Main Site: https://www.canada.ca/en/environment-climate-change/services/national-pollutant-release-inventory.html
- License: Open Government Licence - Canada

### Access Requirements:
- **No authentication required**
- **No API key needed**
- **Completely open access**
- Open Government Licence

### Data Format:
- **Bulk CSV/Excel downloads** (not REST API)
- Annual reports by facility
- Releases, disposals, transfers data
- Chemical pollutants tracked

### Available Data:
- Facility-level reporting
- Chemical substance releases (air, water, land)
- Disposals and transfers
- Multiple years available (2020-2024+)
- CAS numbers included

### Download Methods:

**Direct CSV Download:**
- Go to Open Canada portal
- Download CSV for specific year
- No authentication needed

**Example Data Structure:**
```csv
Facility_Name,CAS_Number,Chemical_Name,Release_Air,Release_Water,Release_Land,Year
Example Plant,50-00-0,Formaldehyde,1000,50,0,2024
```

### Python Example (CSV Processing):
```python
import pandas as pd
import requests

# Download NPRI data
url = "https://open.canada.ca/data/dataset/1fb7d8d4-7713-4ec6-b957-4a882a84fed3/resource/f246e615-826e-42a5-8757-089a5e737f9e/download/npri-inrp_datadonnes_2024.csv"
df = pd.read_csv(url)

# Filter for specific chemical
formaldehyde = df[df['CAS_Number'] == '50-00-0']
print(formaldehyde)
```

### Recommendation for MVP:
✅ **Include for Canadian regulatory data**  
- Download CSV files once
- Process locally
- Cross-reference with PubChem for chemical details  
- Good for Regulation 153 compliance (if applicable)

### Advantages:
✅ Open government data  
✅ No authentication  
✅ Regulatory focus  
✅ Canadian relevance

### Limitations:
⚠ Not a REST API (bulk downloads only)  
⚠ Limited to reported pollutants  
⚠ Annual updates (not real-time)  
⚠ Facility-focused (not comprehensive chemical database)

---

## GitHub Examples & Wrapper Libraries

### 1. PubChemPy (Python) - Most Mature
**Repository:** https://github.com/mcs07/PubChemPy

```python
pip install pubchempy

import pubchempy as pcp

# No API key needed!
compound = pcp.Compound.from_cid(2244)
properties = pcp.get_properties(['MolecularFormula', 'MolecularWeight'],
                               'aspirin', 'name')
```

**Features:**
- Automatic rate limiting
- Caching support
- Error handling
- No authentication needed

---

### 2. ChEMBL Web Resource Client (Python)
**Repository:** https://github.com/chembl/chembl_webresource_client

While focused on ChEMBL (bioactivity database), this library shows good practices and **requires no authentication** for basic ChEMBL access:

```python
pip install chembl-webresource-client

from chembl_webresource_client.new_client import new_client

# No API key needed for ChEMBL
molecule = new_client.molecule
m1 = molecule.get('CHEMBL25')  # Aspirin
print(m1['pref_name'])  # ASPIRIN
```

---

## Implementation Strategy for MVP

### Phase 1: Core Implementation (Week 1)
**Use PubChem + NCI Resolver (Both No Auth)**

```python
class ChemicalMatcher:
    def __init__(self):
        self.pubchem_base = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
        self.nci_base = "https://cactus.nci.nih.gov/chemical/structure"
        
    def search_by_cas(self, cas_number):
        """Search using multiple sources"""
        # Try NCI first (faster for simple lookups)
        nci_result = self._nci_lookup(cas_number)
        
        # Get full details from PubChem
        pubchem_result = self._pubchem_lookup(cas_number)
        
        return {
            'cas': cas_number,
            'inchi_key': nci_result,
            'details': pubchem_result
        }
    
    def _nci_lookup(self, cas_number):
        url = f"{self.nci_base}/{cas_number}/stdinchikey"
        # Add timeout, retry logic
        response = requests.get(url, timeout=5)
        return response.text if response.ok else None
        
    def _pubchem_lookup(self, cas_number):
        url = f"{self.pubchem_base}/compound/name/{cas_number}/JSON"
        # Add rate limiting, caching
        time.sleep(0.2)  # 5 req/sec
        response = requests.get(url, timeout=10)
        return response.json() if response.ok else None
```

### Phase 2: Enhancement (Week 2-3)
1. Add NPRI data (download CSV once)
2. Register for CAS Common Chemistry key
3. Register for ChemSpider key
4. Test and add if useful

### Phase 3: Optimization (Week 4)
1. Implement caching (local SQLite)
2. Add batch processing
3. Error handling and fallbacks
4. Logging and monitoring

---

## Rate Limiting Best Practices

### Recommended Approach:
```python
import time
from functools import wraps

class RateLimiter:
    def __init__(self, calls_per_second=5):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
        
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()
            return func(*args, **kwargs)
        return wrapper

# Usage
@RateLimiter(calls_per_second=5)  # PubChem limit
def query_pubchem(identifier):
    # Your API call here
    pass
```

---

## Caching Strategy

### Use requests-cache for automatic caching:
```python
import requests_cache

# Setup cache (SQLite backend)
requests_cache.install_cache(
    'chemical_cache',
    backend='sqlite',
    expire_after=86400  # 24 hours
)

# All requests now automatically cached
response = requests.get(url)  # First call - hits API
response = requests.get(url)  # Second call - from cache
```

---

## Summary Table

| Database | Auth Required | Registration | Free Tier | Rate Limit | Priority |
|----------|--------------|--------------|-----------|------------|----------|
| **PubChem** | ❌ No | ❌ No | ✅ Yes (Unlimited) | 5 req/sec | ⭐⭐⭐ High |
| **NCI Resolver** | ❌ No | ❌ No | ✅ Yes | None stated | ⭐⭐⭐ High |
| **EPA CompTox** | ⚠ Unclear | ⚠ Maybe | ⚠ Unclear | Unknown | ⏸ Investigate |
| **CAS Common** | ✅ Key | ✅ Yes | ✅ Yes (Limited) | Unknown | ⏸ Secondary |
| **ChemSpider** | ✅ Key | ✅ RSC Account | ✅ Yes | Unknown | ⏸ Secondary |
| **NPRI Canada** | ❌ No | ❌ No | ✅ Yes | N/A (Bulk) | ✅ Include |

---

## Code Examples Repository

### Example 1: Multi-Source Chemical Lookup
```python
import requests
import time
from typing import Optional, Dict, Any

class ChemicalLookup:
    """Multi-source chemical database lookup - NO AUTHENTICATION NEEDED"""
    
    def __init__(self):
        self.sources = {
            'pubchem': 'https://pubchem.ncbi.nlm.nih.gov/rest/pug',
            'nci': 'https://cactus.nci.nih.gov/chemical/structure'
        }
        self.rate_limit_delay = 0.2  # 5 requests/second
        
    def lookup_by_cas(self, cas_number: str) -> Dict[str, Any]:
        """
        Lookup chemical by CAS number using multiple sources
        Returns consolidated data from all sources
        """
        results = {
            'cas_number': cas_number,
            'inchi_key': None,
            'molecular_formula': None,
            'molecular_weight': None,
            'iupac_name': None,
            'synonyms': [],
            'sources_used': []
        }
        
        # Try NCI first for InChI Key (fast and simple)
        try:
            inchi_key = self._nci_get_inchi_key(cas_number)
            if inchi_key:
                results['inchi_key'] = inchi_key
                results['sources_used'].append('nci')
        except Exception as e:
            print(f"NCI lookup failed: {e}")
        
        # Get detailed info from PubChem
        try:
            pubchem_data = self._pubchem_get_compound(cas_number)
            if pubchem_data:
                results.update(pubchem_data)
                results['sources_used'].append('pubchem')
        except Exception as e:
            print(f"PubChem lookup failed: {e}")
        
        return results
    
    def _nci_get_inchi_key(self, identifier: str) -> Optional[str]:
        """Get InChI Key from NCI Chemical Identifier Resolver"""
        url = f"{self.sources['nci']}/{identifier}/stdinchikey"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            return response.text.strip()
        return None
    
    def _pubchem_get_compound(self, identifier: str) -> Optional[Dict]:
        """Get compound details from PubChem"""
        # Respect rate limit
        time.sleep(self.rate_limit_delay)
        
        # Try by name/CAS
        url = f"{self.sources['pubchem']}/compound/name/{identifier}/property/MolecularFormula,MolecularWeight,IUPACName,InChIKey/JSON"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'PropertyTable' in data and 'Properties' in data['PropertyTable']:
                props = data['PropertyTable']['Properties'][0]
                return {
                    'molecular_formula': props.get('MolecularFormula'),
                    'molecular_weight': props.get('MolecularWeight'),
                    'iupac_name': props.get('IUPACName'),
                    'inchi_key': props.get('InChIKey')
                }
        return None

# Usage example
lookup = ChemicalLookup()
result = lookup.lookup_by_cas('50-00-0')  # Formaldehyde
print(result)
```

### Example 2: Batch Processing with Caching
```python
import requests
import requests_cache
from typing import List, Dict
import pandas as pd
import time

# Install cache
requests_cache.install_cache('chemical_cache', expire_after=86400)

class BatchChemicalMatcher:
    """Process multiple chemicals efficiently with caching"""
    
    def __init__(self):
        self.pubchem_base = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
        self.results = []
        
    def process_cas_list(self, cas_numbers: List[str]) -> pd.DataFrame:
        """Process list of CAS numbers"""
        for cas in cas_numbers:
            print(f"Processing {cas}...")
            result = self._lookup_single(cas)
            self.results.append(result)
            time.sleep(0.2)  # Rate limiting
            
        return pd.DataFrame(self.results)
    
    def _lookup_single(self, cas: str) -> Dict:
        """Lookup single chemical"""
        url = f"{self.pubchem_base}/compound/name/{cas}/property/MolecularFormula,MolecularWeight/JSON"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                props = data['PropertyTable']['Properties'][0]
                return {
                    'cas': cas,
                    'found': True,
                    'formula': props.get('MolecularFormula'),
                    'weight': props.get('MolecularWeight'),
                    'from_cache': response.from_cache if hasattr(response, 'from_cache') else False
                }
        except Exception as e:
            return {
                'cas': cas,
                'found': False,
                'error': str(e)
            }
        
        return {'cas': cas, 'found': False}

# Usage
matcher = BatchChemicalMatcher()
cas_list = ['50-00-0', '67-56-1', '64-17-5']  # Formaldehyde, Methanol, Ethanol
results_df = matcher.process_cas_list(cas_list)
results_df.to_csv('chemical_matches.csv', index=False)
```

---

## Recommendations for MVP Development

### ✅ **Start Immediately With:**
1. **PubChem API** - no auth, excellent coverage, well-documented  
2. **NCI Chemical Identifier Resolver** - no auth, great for conversions  
3. **PubChemPy Python library** - handles rate limiting and caching

### ⏸ **Register Now, Implement Later:**
1. **CAS Common Chemistry** - register to get key for future  
2. **ChemSpider (RSC)** - register to get key for future

### 🔍 **Investigate Further:**
1. **EPA CompTox Dashboard** - contact EPA for API documentation  
2. Test if direct API endpoints exist

### 📥 **Download Once:**
1. **NPRI Data** - download CSV files for Canadian regulatory data

### 🚫 **Avoid for MVP:**
- Web scraping (legal/ethical issues)
- Paid commercial databases (CAS Registry, SciFinder)
- Complex authentication systems

---

## Testing Checklist

### Before Implementation:
- [ ] Test PubChem API response times
- [ ] Verify NCI Resolver uptime
- [ ] Download sample NPRI data
- [ ] Register for CAS Common Chemistry key
- [ ] Register for ChemSpider/RSC account
- [ ] Set up requests-cache  
- [ ] Implement rate limiting wrapper
- [ ] Test error handling for 404/500 errors
- [ ] Validate data quality from each source

---

## Conclusion

**For MVP implementation, you can start TODAY with zero authentication barriers:**

1. **PubChem** provides comprehensive chemical data with no API key
2. **NCI Resolver** offers fast identifier conversion with no registration
3. Both services are free, government-funded, and highly reliable

**The only "keys" you need are URLs and patience (rate limiting).**

Secondary sources (ChemSpider, CAS Common Chemistry) can be added after initial MVP is working, once you obtain free API keys through simple registration forms.

The recommended architecture is:
```
Primary: PubChem (comprehensive data) + NCI (fast conversions)
Secondary: NPRI (CSV download for Canadian regulations)
Future: CAS Common Chemistry + ChemSpider (after registration)
```

This approach delivers maximum functionality with minimal complexity and zero cost.

---

## Additional Resources

### Official Documentation:
- PubChem PUG REST: https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest
- PubChem Tutorial: https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest-tutorial
- NCI Services: https://cactus.nci.nih.gov/
- EPA CompTox: https://www.epa.gov/chemical-research/chemistry-dashboard

### Python Libraries:
- PubChemPy: https://pypi.org/project/PubChemPy/
- Requests-cache: https://pypi.org/project/requests-cache/
- Pandas: https://pandas.pydata.org/

### Example Projects:
- PubChemPy Examples: https://github.com/mcs07/PubChemPy/tree/main/examples
- ChEMBL Client: https://github.com/chembl/chembl_webresource_client

---

**Report Compiled By:** Chemical Database API Research  
**Date:** February 12, 2026  
**Status:** Ready for MVP Implementation  
**Next Steps:** Begin with PubChem + NCI integration, no authentication barriers
