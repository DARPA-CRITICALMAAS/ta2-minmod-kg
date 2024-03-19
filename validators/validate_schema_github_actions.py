import json
import jsonschema
import os
import create_ttl_files
import validators
import sys
import generate_uris
import validator_utils


def validate_json_schema(json_data):

    schema = validator_utils.mineral_site_schema()
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

    schema = validator_utils.mineral_system_schema()
    json_string = json.dumps(json_data)
    mineral_system_json = json.loads(json_string)

    try:
        jsonschema.validate(instance=mineral_system_json, schema=schema)
        print("Validation succeeded")
    except jsonschema.ValidationError as e:
        print(f"Validation failed: {e}")
        raise  # Raise an exception to indicate failure

    return json_data


def add_id_to_mineral_site(json_data, base_path):
    ms_list = json_data['MineralSite']
    mndr_url = 'https://minmod.isi.edu/resource/'

    for ms in ms_list:
        if "deposit_type_candidate" in ms:
            for dp in ms['deposit_type_candidate']:
                if 'normalized_uri' in dp:
                    validator_utils.is_valid_uri(dp['normalized_uri'])
                dp['id'] = mndr_url + validator_utils.deposit_uri(dp)

        ms['id'] = mndr_url + validator_utils.mineral_site_uri(ms)
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
                        validator_utils.is_valid_uri(dp)

                if "commodity" in mi:
                    validator_utils.is_valid_uri(mi['commodity'])

                if "ore" in mi:
                    if "ore_unit" in mi['ore']:
                        ore = mi['ore']
                        validator_utils.is_valid_uri(ore['ore_unit'])

                if "grade" in mi:
                    if "grade_unit" in mi['grade']:
                        grade = mi['grade']
                        validator_utils.is_valid_uri(grade['grade_unit'])

                if "cutoff_grade" in mi:
                    if "grade_unit" in mi['cutoff_grade']:
                        cutoff_grade = mi['cutoff_grade']
                        validator_utils.is_valid_uri(cutoff_grade['grade_unit'])

                mi_data = {
                    "site": ms,
                    "id": counter
                }
                mi['id'] = mndr_url + validator_utils.mineral_inventory_uri(mi_data)
                counter += 1

                if "reference" in mi:
                    reference = mi['reference']
                    if "document" in reference:
                        document = reference['document']
                        doc_data = {
                            "document": document
                        }
                        document['id'] = mndr_url + validator_utils.document_uri(doc_data)


    # filename = get_filename(file_path)
    with open(file_path, 'w') as file:
        # Write the new data to the file
        file.write(json.dumps(json_data, indent=2) + '\n')
    create_ttl_files.create_drepr_from_mineral_site(file_path, base_path)



def add_id_to_mineral_system(json_data, base_path):
    ms_list = json_data['MineralSystem']
    mndr_url = 'https://minmod.isi.edu/resource/'

    for ms in ms_list:
        if "deposit_type" in ms:
            for dp in ms['deposit_type']:
                validator_utils.is_valid_uri(dp)
        ms['id'] = mndr_url + validator_utils.mineral_system_uri(ms)

        fields = ['source', 'pathway', 'trap', 'preservation', 'energy', 'outflow']

        for f in fields:

            if f in ms:
                for f_object in ms[f]:
                    if "supporting_references" in f_object:
                        for reference in f_object['supporting_references']:
                            if "document" in reference:
                                document = reference['document']
                                doc_data = {
                                    "document": document
                                }
                                document['id'] = mndr_url + validator_utils.document_uri(doc_data)


    # filename = get_filename(file_path)
    with open(file_path, 'w') as file:
        # Write the new data to the file
        file.write(json.dumps(json_data, indent=2) + '\n')
    create_ttl_files.create_drepr_from_mineral_system(file_path, base_path)


changed_files = sys.argv[1]
temp_file = sys.argv[2]
base_path = sys.argv[3]

file_path = changed_files

# with open(file_path, 'r') as file:
#     # Write the new data to the file
#     print(file.read())

print(file_path)
if validator_utils.is_json_file_under_data(file_path):
    print(f'{file_path} is a JSON file, running validation on it')
    json_data = {}
    try:
        with open(file_path, 'r') as file:
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

    json_string = json.dumps(json_data)
    json_string = validator_utils.remove_non_printable_chars(json_string)
    # print('\n'.join(json_string.split('\n')[:3]))
    # print(json_string)

    json_data = json.loads(json_string)
    print('Json validated ...')
    # print(json_string)


    if 'MineralSite' in json_data:
        json_data = add_id_to_mineral_site(json_data, base_path)
    elif 'MineralSystem' in json_data:
        json_data = add_id_to_mineral_system(json_data, base_path)
else:
    print(f'{file_path} is not a JSON file')

