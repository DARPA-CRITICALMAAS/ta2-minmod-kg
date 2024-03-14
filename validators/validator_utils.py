import os
import generate_uris
import validators
import requests

def is_valid_uri(uri):
    return validators.url(uri)

def mineral_site_uri(data):
    response = generate_uris.mineral_site_uri(data)
    uri = response['result']
    return uri

def deposit_uri(data):
    response = generate_uris.deposit_type_uri(data)
    uri = response['result']
    return uri
def document_uri(data):
    response = generate_uris.document_uri(data)
    uri = response['result']
    return uri


def mineral_system_uri(data):
    response = generate_uris.mineral_system_uri(data)
    uri = response['result']
    return uri

def mineral_inventory_uri(param1):
    response = generate_uris.mineral_inventory_uri(param1)
    uri = response['result']
    return uri


def remove_non_printable_chars(text):
    clean_text = text.replace('\n', ' ').replace('\\u000b', '').replace('\\"', ' ')
    return clean_text


def is_json_file_under_data(file_path):
    path, file_extension = os.path.splitext(file_path)
    split_path = path.split('/')
    is_under_data_folder = False
    if len(split_path) > 0:
        if (len(split_path) > 3 and split_path[-4] == 'data' and split_path[-3] == 'inferlink' and split_path[-2] == 'extractions') \
                or (len(split_path) > 2 and split_path[-2] == 'umn') or (len(split_path) > 2 and (split_path[-2] == 'sri' or split_path[-2] == 'mappableCriteria')):
            is_under_data_folder = True

    return is_under_data_folder and file_extension.lower() == '.json'

def get_filename(file_path):
    path, file_extension = os.path.splitext(file_path)
    split_path = path.split('/')
    if len(path) > 0:
        return split_path[-1]


def mineral_site_schema():
    schema = {
        "type": "object",
        "properties" : {
            "MineralSite": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties" : {
                        "id" :  {"type": ["string", "number"]},
                        "name" : {"type" : "string"},
                        "source_id" : {"type" : "string"},
                        "record_id" : {"type": ["string", "number"]},
                        "location_info": {
                            "type": "object",
                            "properties": {
                                "location": {"type": "string"},
                                "country": {"type": "string"},
                                "state_or_province": {"type": "string"},
                                "location_source_record_id": {"type": "string"},
                                "crs": {"type": "string"},
                                "location_source": {"type": "string"}
                            }
                        },
                        "deposit_type_candidate" : {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "observed_name": {"type": "string"},
                                    "source": {"type": "string"},
                                    "normalized_uri": {"type": "string"},
                                    "confidence": {"type": "number"}
                                }
                            }
                        },
                        "geology_info": {
                            "type": "object",
                            "properties": {
                                "age": {"type": "string"},
                                "unit_name": {"type": "string"},
                                "description": {"type": "string"},
                                "lithology": {"type": "string"},
                                "process": {"type": "string"},
                                "comments": {"type": "string"},
                                "environment": {"type": "string"}
                            }
                        },
                        "MineralInventory": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": ["string", "number"]},
                                    "category": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    },
                                    "contained_metal": {"type": "number"},
                                    "reference": {
                                        "type": "object",
                                        "properties": {
                                            "document": {
                                                "type": "object",
                                                "properties": {
                                                    "id":  {"type": ["string", "number"]},
                                                    "title": {"type": "string"},
                                                    "doi": {"type": "string"},
                                                    "uri": {"type": "string"},
                                                    "journal": {"type": "string"},
                                                    "year": {"type": "number"},
                                                    "month": {"type": "number"},
                                                    "volume": {"type": "number"},
                                                    "issue": {"type": "number"},
                                                    "description": {"type": "string"},
                                                    "authors": {
                                                        "type": "array",
                                                        "items": {"type": "string"}
                                                    }
                                                }
                                            },
                                            "page_info": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "page": {"type": "number"},
                                                        "bounding_box": {
                                                            "type": "object",
                                                            "properties": {
                                                                "x_min": {"type": ["string", "number"]},
                                                                "x_max": {"type": ["string", "number"]},
                                                                "y_min": {"type": ["string", "number"]},
                                                                "y_max": {"type": ["string", "number"]}
                                                            }
                                                        }
                                                    },
                                                    "required": ["page"]
                                                }

                                            }
                                        }
                                    },
                                    "date": {"type": "string", "format": "date"},
                                    "commodity": {"type": "string"},
                                    "ore": {
                                        "type": "object",
                                        "properties": {
                                            "ore_unit": {"type": "string"},
                                            "ore_value": {"type": "number"}
                                        }
                                    },
                                    "grade": {
                                        "type": "object",
                                        "properties": {
                                            "grade_unit": {"type": "string"},
                                            "grade_value": {"type": "number"}
                                        }

                                    },
                                    "cutoff_grade": {
                                        "type": "object",
                                        "properties": {
                                            "grade_unit": {"type": "string"},
                                            "grade_value": {"type": "number"}
                                        }
                                    }
                                },
                                "required": ["reference"]
                            }
                        }
                    }
                    ,
                    "required": ["source_id", "record_id"]
                }
            }
        }
    }
    return schema


def mineral_system_schema():
    schema = {
        "type": "object",
        "properties" : {
            "MineralSystem": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties" : {

                        "source" : {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "criteria": {"type": "string"},
                                    "theorectical": {"type": "string"},
                                    "potential_dataset": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "relevance_score": {"type": "number"}
                                            }
                                        }
                                    },
                                    "supporting_references": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "document": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id":  {"type": ["string", "number"]},
                                                        "title": {"type": "string"},
                                                        "doi": {"type": "string"},
                                                        "uri": {"type": "string"},
                                                        "journal": {"type": "string"},
                                                        "year": {"type": "number"},
                                                        "month": {"type": "number"},
                                                        "volume": {"type": "number"},
                                                        "issue": {"type": "number"},
                                                        "description": {"type": "string"},
                                                        "authors": {
                                                            "type": "array",
                                                            "items": {"type": "string"}
                                                        }
                                                    }
                                                },
                                                "page_info": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "page": {"type": "number"},
                                                            "bounding_box": {
                                                                "type": "object",
                                                                "properties": {
                                                                    "x_min": {"type": ["string", "number"]},
                                                                    "x_max": {"type": ["string", "number"]},
                                                                    "y_min": {"type": ["string", "number"]},
                                                                    "y_max": {"type": ["string", "number"]}
                                                                }
                                                            }
                                                        },
                                                        "required": ["page"]
                                                    }

                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "trap" : {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "criteria": {"type": "string"},
                                    "theorectical": {"type": "string"},
                                    "potential_dataset": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "relevance_score": {"type": "number"}
                                            }
                                        }
                                    },
                                    "supporting_references": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "document": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id":  {"type": ["string", "number"]},
                                                        "title": {"type": "string"},
                                                        "doi": {"type": "string"},
                                                        "uri": {"type": "string"},
                                                        "journal": {"type": "string"},
                                                        "year": {"type": "number"},
                                                        "month": {"type": "number"},
                                                        "volume": {"type": "number"},
                                                        "issue": {"type": "number"},
                                                        "description": {"type": "string"},
                                                        "authors": {
                                                            "type": "array",
                                                            "items": {"type": "string"}
                                                        }
                                                    }
                                                },
                                                "page_info": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "page": {"type": "number"},
                                                            "bounding_box": {
                                                                "type": "object",
                                                                "properties": {
                                                                    "x_min": {"type": ["string", "number"]},
                                                                    "x_max": {"type": ["string", "number"]},
                                                                    "y_min": {"type": ["string", "number"]},
                                                                    "y_max": {"type": ["string", "number"]}
                                                                }
                                                            }
                                                        },
                                                        "required": ["page"]
                                                    }

                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "preservation" : {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "criteria": {"type": "string"},
                                    "theorectical": {"type": "string"},
                                    "potential_dataset": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "relevance_score": {"type": "number"}
                                            }
                                        }
                                    },
                                    "supporting_references": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "document": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id":  {"type": ["string", "number"]},
                                                        "title": {"type": "string"},
                                                        "doi": {"type": "string"},
                                                        "uri": {"type": "string"},
                                                        "journal": {"type": "string"},
                                                        "year": {"type": "number"},
                                                        "month": {"type": "number"},
                                                        "volume": {"type": "number"},
                                                        "issue": {"type": "number"},
                                                        "description": {"type": "string"},
                                                        "authors": {
                                                            "type": "array",
                                                            "items": {"type": "string"}
                                                        }
                                                    }
                                                },
                                                "page_info": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "page": {"type": "number"},
                                                            "bounding_box": {
                                                                "type": "object",
                                                                "properties": {
                                                                    "x_min": {"type": ["string", "number"]},
                                                                    "x_max": {"type": ["string", "number"]},
                                                                    "y_min": {"type": ["string", "number"]},
                                                                    "y_max": {"type": ["string", "number"]}
                                                                }
                                                            }
                                                        },
                                                        "required": ["page"]
                                                    }

                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "pathway" : {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "criteria": {"type": "string"},
                                    "theorectical": {"type": "string"},
                                    "potential_dataset": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "relevance_score": {"type": "number"}
                                            }
                                        }
                                    },
                                    "supporting_references": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "document": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id":  {"type": ["string", "number"]},
                                                        "title": {"type": "string"},
                                                        "doi": {"type": "string"},
                                                        "uri": {"type": "string"},
                                                        "journal": {"type": "string"},
                                                        "year": {"type": "number"},
                                                        "month": {"type": "number"},
                                                        "volume": {"type": "number"},
                                                        "issue": {"type": "number"},
                                                        "description": {"type": "string"},
                                                        "authors": {
                                                            "type": "array",
                                                            "items": {"type": "string"}
                                                        }
                                                    }
                                                },
                                                "page_info": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "page": {"type": "number"},
                                                            "bounding_box": {
                                                                "type": "object",
                                                                "properties": {
                                                                    "x_min": {"type": ["string", "number"]},
                                                                    "x_max": {"type": ["string", "number"]},
                                                                    "y_min": {"type": ["string", "number"]},
                                                                    "y_max": {"type": ["string", "number"]}
                                                                }
                                                            }
                                                        },
                                                        "required": ["page"]
                                                    }

                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "outflow" : {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "criteria": {"type": "string"},
                                    "theorectical": {"type": "string"},
                                    "potential_dataset": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "relevance_score": {"type": "number"}
                                            }
                                        }
                                    },
                                    "supporting_references": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "document": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id":  {"type": ["string", "number"]},
                                                        "title": {"type": "string"},
                                                        "doi": {"type": "string"},
                                                        "uri": {"type": "string"},
                                                        "journal": {"type": "string"},
                                                        "year": {"type": "number"},
                                                        "month": {"type": "number"},
                                                        "volume": {"type": "number"},
                                                        "issue": {"type": "number"},
                                                        "description": {"type": "string"},
                                                        "authors": {
                                                            "type": "array",
                                                            "items": {"type": "string"}
                                                        }
                                                    }
                                                },
                                                "page_info": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "page": {"type": "number"},
                                                            "bounding_box": {
                                                                "type": "object",
                                                                "properties": {
                                                                    "x_min": {"type": ["string", "number"]},
                                                                    "x_max": {"type": ["string", "number"]},
                                                                    "y_min": {"type": ["string", "number"]},
                                                                    "y_max": {"type": ["string", "number"]}
                                                                }
                                                            }
                                                        },
                                                        "required": ["page"]
                                                    }

                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "energy" : {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "criteria": {"type": "string"},
                                    "theorectical": {"type": "string"},
                                    "potential_dataset": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "relevance_score": {"type": "number"}
                                            }
                                        }
                                    },
                                    "supporting_references": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "document": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id":  {"type": ["string", "number"]},
                                                        "title": {"type": "string"},
                                                        "doi": {"type": "string"},
                                                        "uri": {"type": "string"},
                                                        "journal": {"type": "string"},
                                                        "year": {"type": "number"},
                                                        "month": {"type": "number"},
                                                        "volume": {"type": "number"},
                                                        "issue": {"type": "number"},
                                                        "description": {"type": "string"},
                                                        "authors": {
                                                            "type": "array",
                                                            "items": {"type": "string"}
                                                        }
                                                    }
                                                },
                                                "page_info": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "page": {"type": "number"},
                                                            "bounding_box": {
                                                                "type": "object",
                                                                "properties": {
                                                                    "x_min": {"type": ["string", "number"]},
                                                                    "x_max": {"type": ["string", "number"]},
                                                                    "y_min": {"type": ["string", "number"]},
                                                                    "y_max": {"type": ["string", "number"]}
                                                                }
                                                            }
                                                        },
                                                        "required": ["page"]
                                                    }

                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    },
                    "required" : ["source", "pathway"]

                }
            }
        }
    }
    return schema

def download_file_with_git_lfs(oid, size, branch):
    url = "https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data.git/info/lfs/objects/batch"
    headers = {
        "Accept": "application/vnd.git-lfs+json",
        "Content-Type": "application/vnd.git-lfs+json",
    }
    data = {
        "operation": "download",
        "ref": {"name": f"refs/heads/{branch}"},
        "objects": [
            {
                "oid": f"{oid}",
                "size": size
            }
        ],
        "hash_algo": "sha256"
    }
    print('Pulling data from batch API')
    response = requests.post(url, json=data, headers=headers)
    print('Pulled data from batch API')
    response_json = response.json()

    download_url = response_json['objects'][0]['actions']['download']['href']
    response = requests.get(download_url)
    file_content = response.text

    return file_content


# def get_lfs_objects(file_content, branch):
#     metadata = file_content.split('\n')
#     oid_line = next((line for line in metadata if line.startswith("oid")), None)
#     size_line = next((line for line in metadata if line.startswith("size")), None)
#     if oid_line:
#         oid = oid_line.split(" ")[-1][7:]
#         size = size_line.split(" ")[-1]
#         file_content = download_file_with_git_lfs(oid, int(size), branch)
#         return file_content
#     else:
#         print('Invalid response ', file_content)
#         raise