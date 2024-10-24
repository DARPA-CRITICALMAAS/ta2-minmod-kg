# Overview

![KG ontology summary](./er_diagram.png)

# Changelog

## [2.0.0] - 2024-09-29

### Added

- [Reference](https://minmod.isi.edu/ontology/Reference) class:
    + [comment](https://minmod.isi.edu/ontology/comment): (optional) This field provides any additional information or clarification about the reference. It serves as an optional annotation that can offer context or specific details related to the reference.
    + [property](https://minmod.isi.edu/ontology/property): (optional) This field specifies what property or field the reference applies to. If this field is missing, the reference is considered to be relevant to all fields of the corresponding entity
- [MineralSite](https://minmod.isi.edu/ontology/MineralSite):
    + [modified_at](https://minmod.isi.edu/ontology/modified_at): This property records the time when the entity was last modified. The value of this property is a UTC datetime in the ISO format `YYYY-MM-DDTHH:MM:SSZ` (e.g., 2024-10-23T14:30:00Z).
    + [created_by](https://minmod.isi.edu/ontology/created_by): This property indicates the user who originally created the entity. The format for this property is a URL in the form `https://minmod.isi.edu/users/{username}`, where `{username}` is replaced by the specific user’s identifier.
    + [reference](https://minmod.isi.edu/ontology/reference): This property contains a list of references, indicating the sources from which the information for the site is derived. It is a list because each reference can be for each individual field of the site, allowing for more granular source of information.

### Changed

- [LocationInfo](https://minmod.isi.edu/ontology/LocationInfo) class:
    + [country](https://minmod.isi.edu/ontology/country):  This property now contains a list of countries instead of a single country. This change allows for cases where a location spans multiple countries
    + [state_or_province](https://minmod.isi.edu/ontology/state_or_province): This property now contains a list of states or provinces instead of a single state or province. This update accommodates cases where a location spans multiple states or provinces.
- [MineralSite](https://minmod.isi.edu/ontology/MineralSite) class:
    + [source_id](https://minmod.isi.edu/ontology/source_id): We introduce three prefixes (`mining-report::`, `article::`, `database::`) to specify the source type. See [source_score.csv](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/blob/main/data/entities/source_score.csv) for examples of source ids.
- Each source is now assigned a score representing its reliability. The score for a source is determined by first prefix matching of the source ID with an entry in the [source_score.csv](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/blob/main/data/entities/source_score.csv).csv file. If no prefix matches the source ID, a default score of 5.0 is applied.
- Remove the top-level `MineralSite` key from the input JSON files containing mineral sites: Instead of wrapping the site data within a MineralSite key, the input JSON now directly contains an array of site objects.
    + Below is an example of the JSON input for mineral sites:
        ```json
        [
            {
                "modified_at": "2024-10-22T03:54:38Z",
                "source_id": "database::https://doi.org/10.5066/P96MMRFD",
                "reference": [
                    {
                        "document": {
                            "uri": "https://doi.org/10.5066/P96MMRFD"
                        }
                    }
                ],
                "name": "Unnamed (ridge at head of Spruce Creek)",
                "mineral_inventory": [
                    {
                        "commodity": {
                            "observed_name": "Ag",
                            "confidence": 1.0,
                            "source": "UMN Matching System v1",
                            "normalized_uri": "https://minmod.isi.edu/resource/Q585"
                        },
                        "reference": {
                            "document": {
                                "uri": "https://doi.org/10.5066/P96MMRFD"
                            }
                        }
                    }
                ],
                "created_by": "https://minmod.isi.edu/users/umn",
                "site_type": "Occurrence",
                "record_id": "MM062",
                "location_info": {
                    "crs": {
                        "observed_name": "EPSG:4267",
                        "confidence": 1.0,
                        "source": "UMN Matching System v1",
                        "normalized_uri": "https://minmod.isi.edu/resource/Q702"
                    },
                    "state_or_province": [
                        {
                            "observed_name": "Alaska",
                            "confidence": 1.0,
                            "source": "UMN Matching System v1",
                            "normalized_uri": "https://minmod.isi.edu/resource/Q6840"
                        }
                    ],
                    "location": "POINT (-150.7248 63.5792)",
                    "country": [
                        {
                            "observed_name": "United States",
                            "confidence": 1.0,
                            "source": "UMN Matching System v1",
                            "normalized_uri": "https://minmod.isi.edu/resource/Q1235"
                        }
                    ]
                }
            }
        ]
        ```
