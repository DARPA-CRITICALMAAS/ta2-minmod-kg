import json
import jsonschema
import sys
import os
import generate_uris
import validators

# TODO: This file is going to be deprecated

def is_valid_uri(uri):
    return validators.url(uri)

def mineral_site_uri(data):
    response = generate_uris.mineral_site_uri(data)
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


def validate_json_schema(json_data):

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
                                                            },
                                                            "required": ["x_min", "x_max", "y_min", "y_max"]
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

    json_string = json.dumps(json_data)
    mineral_site_json = json.loads(json_string)

    try:
        jsonschema.validate(instance=mineral_site_json, schema=schema)
        print("Validation succeeded")
    except jsonschema.ValidationError as e:
        print(f"Validation failed: {e}")
        raise  # Raise an exception to indicate failure

    return json_data



def validate_json_schema_mineral_system(json_data):

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
                                                                },
                                                                "required": ["x_min", "x_max", "y_min", "y_max"]
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
                                                                },
                                                                "required": ["x_min", "x_max", "y_min", "y_max"]
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
                                                                },
                                                                "required": ["x_min", "x_max", "y_min", "y_max"]
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
                                                                },
                                                                "required": ["x_min", "x_max", "y_min", "y_max"]
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
                                                                },
                                                                "required": ["x_min", "x_max", "y_min", "y_max"]
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
                                                                },
                                                                "required": ["x_min", "x_max", "y_min", "y_max"]
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

    json_string = json.dumps(json_data)
    mineral_system_json = json.loads(json_string)

    try:
        jsonschema.validate(instance=mineral_system_json, schema=schema)
        print("Validation succeeded")
    except jsonschema.ValidationError as e:
        print(f"Validation failed: {e}")
        raise  # Raise an exception to indicate failure

    return json_data
def add_id_to_mineral_site(json_data, new_json_folder, file_name_without_path):
    ms_list = json_data['MineralSite']
    mndr_url = 'https://minmod.isi.edu/resource/'

    for ms in ms_list:
        if "deposit_type_candidate" in ms:
            for dp in ms['deposit_type_candidate']:
                is_valid_uri(dp['normalized_uri'])
                dp['id'] = mndr_url + deposit_uri(dp)

        ms['id'] = mndr_url + mineral_site_uri(ms)
        if "location_info" in ms:
            ll = ms["location_info"]
            if "state_or_province" in ll and ll["state_or_province"] is None:
                ll["state_or_province"] = ""

        if "MineralInventory" in ms:
            mi_list = ms['MineralInventory']
            counter = 0

            for mi in mi_list:
                if "category" in mi:
                    for dp in mi['category']:
                        is_valid_uri(dp)

                if "commodity" in mi:
                    is_valid_uri(mi['commodity'])

                if "ore" in mi:
                    if "ore_unit" in mi['ore']:
                        ore = mi['ore']
                        is_valid_uri(ore['ore_unit'])

                if "grade" in mi:
                    if "grade_unit" in mi['grade']:
                        grade = mi['grade']
                        is_valid_uri(grade['grade_unit'])

                if "cutoff_grade" in mi:
                    if "grade_unit" in mi['cutoff_grade']:
                        cutoff_grade = mi['cutoff_grade']
                        is_valid_uri(cutoff_grade['grade_unit'])

                mi_data = {
                    "site": ms,
                    "id": counter
                }
                mi['id'] = mndr_url + mineral_inventory_uri(mi_data)
                counter += 1

                if "reference" in mi:
                    reference = mi['reference']
                    if "document" in reference:
                        document = reference['document']
                        doc_data = {
                            "document": document
                        }
                        document['id'] = mndr_url + document_uri(doc_data)
                        print(document['id'])


    file_to_write = new_json_folder + '/' + file_name_without_path
    file_exists = os.path.exists(file_to_write)

    if not file_exists:
        os.makedirs(os.path.dirname(file_to_write), exist_ok=True)

    # print(json.dumps(json_data, indent=2))

    with open(file_to_write, 'w') as file:
        file.write(json.dumps(json_data, indent=2))



def add_id_to_mineral_system(json_data, new_json_folder, file_name_without_path):
    ms_list = json_data['MineralSystem']
    mndr_url = 'https://minmod.isi.edu/resource/'

    for ms in ms_list:
        if "deposit_type" in ms:
            for dp in ms['deposit_type']:
                is_valid_uri(dp)
        ms['id'] = mndr_url + mineral_system_uri(ms)
        fields = ['source', 'pathway', 'trap', 'preservation', 'energy', 'outflow']

        for f in fields:
            if f in ms:
                for f_object in ms[f]:
                    if "supporting_references" in f_object:
                        print(f_object['supporting_references'])
                        for reference in f_object['supporting_references']:
                            if "document" in reference:
                                document = reference['document']
                                doc_data = {
                                    "document": document
                                }
                                document['id'] = mndr_url + document_uri(doc_data)
                                print(document['id'])


    file_to_write = new_json_folder + '/' + file_name_without_path
    file_exists = os.path.exists(file_to_write)

    if not file_exists:
        os.makedirs(os.path.dirname(file_to_write), exist_ok=True)

    # print(json.dumps(json_data, indent=2))
    with open(file_to_write, 'w') as file:
        file.write(json.dumps(json_data, indent=2))



filename = sys.argv[1]
new_json_folder = sys.argv[2]
file_name_without_path = os.path.basename(filename)

file_path = filename

print(filename, new_json_folder)

print(f'{file_path} is a JSON file, running validation on it')
json_data = {}
try:
    with open(filename) as file:
        json_data = json.load(file)
    if 'MineralSite' in json_data:
        json_data = validate_json_schema(json_data)
    elif 'MineralSystem' in json_data:
        json_data = validate_json_schema_mineral_system(json_data)
except FileNotFoundError:
    print(f"File '{file_path}' was deleted, skipping.")
    sys.exit(0)
except Exception as e:
    print(f"An error occurred: {e}")
    raise

if 'MineralSite' in json_data:
    json_data = add_id_to_mineral_site(json_data, new_json_folder, file_name_without_path)
elif 'MineralSystem' in json_data:
    json_data = add_id_to_mineral_system(json_data, new_json_folder, file_name_without_path)
