# MinMod API

MinMod API to create/update mineral sites.

## Quick start

1. Login to MinMod first. You can use the following command: `python -m minmodapi login -u <username> [-e <endpoint>]`. By default, the endpoint is `https://dev.minmod.isi.edu`, for production use `https://minmod.isi.edu`.

2. Upload mineral sites to MinMod programmatically:

```python

from minmodapi import MinModAPI, merge_deposit_type, replace_site

api = MinModAPI(<endpoint>)

# prepare a mineral site to upload
mineral_site = {
    "name": "test_mineral_site",
    "source_id": "https://api.cdr.land/v1/docs/documents",
    "record_id":  "021c30faa880995b3cab0fcdaa9ea2fa895634205ad3977b664f33492b2086052c"
    "location_info": {
        "country": [],
        "state_or_province": []
    },
    "mineral_inventory": [],
    "deposit_type_candidate": [],
    "modified_at": "2024-11-15T18:38:01.050553Z",
    ...
}

# Upload the mineral site. The function automatically determines whether
# to create a new site or update an existing one. If updating, the
# `apply_update` function merges the new site with the existing one.
# For instance, `replace_site` replaces the existing site with the new site.
# This function returns the identifier of the mineral site, or None if the upload fails.
mineral_site_ident = api.upsert_mineral_site(mineral_site, apply_update=replace_site)

# If not None, use `get_browse_link` or `get_api_link` to retrieve the site link.
if mineral_site_ident is not None:
    print("LOD link:", mineral_site_ident.get_browse_link())
    print("API link:", mineral_site_ident.get_api_link())
```
