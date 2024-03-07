import json
import jsonschema
import os
import create_ttl_files
import validators
import sys
import generate_uris
import validator_utils
import requests
import base64

def read_file_from_github(owner, repo, path_to_file, token):
    url = f"https://raw.githubusercontent.com/{owner}/{repo}/github-actions-for-validation/{path_to_file}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Parse the response JSON
    file_info = response.json()

    # Decode the Base64-encoded content
    content = base64.b64decode(file_info['content']).decode('utf-8')

    return content

def is_valid_uri(uri):
    return validators.url(uri)

def remove_non_printable_chars(text):
    clean_text = text.replace('\n', ' ').replace('\\u000b', '').replace('\\n', ' ')
    return clean_text

def mineral_site_uri(data):
    response = generate_uris.mineral_site_uri(data)
    uri = response['result']
    return uri


def mineral_system_uri(data):
    response = generate_uris.mineral_system_uri(data)
    uri = response['result']
    return uri
def document_uri(data):
    response = generate_uris.document_uri(data)
    uri = response['result']
    return uri

def deposit_uri(data):
    response = generate_uris.deposit_type_uri(data)
    uri = response['result']
    return uri

def mineral_inventory_uri(param1):
    response = generate_uris.mineral_inventory_uri(param1)
    uri = response['result']
    return uri

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


def add_id_to_mineral_site(json_data):
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


    filename = get_filename(file_path)
    with open(file_path, 'w') as file:
        # Write the new data to the file
        file.write(json.dumps(json_data, indent=2) + '\n')
    create_ttl_files.create_drepr_from_mineral_site(file_path, filename)



def add_id_to_mineral_system(json_data):
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
                        for reference in f_object['supporting_references']:
                            if "document" in reference:
                                document = reference['document']
                                doc_data = {
                                    "document": document
                                }
                                document['id'] = mndr_url + document_uri(doc_data)


    filename = get_filename(file_path)
    with open(file_path, 'w') as file:
        # Write the new data to the file
        file.write(json.dumps(json_data, indent=2) + '\n')
    create_ttl_files.create_drepr_from_mineral_system(file_path, filename)


changed_files = sys.argv[1]
temp_file = sys.argv[2]
file_path = changed_files

# Example usage
owner = 'DARPA-CRITICALMAAS'  # Replace 'username' with the GitHub username of the repository owner
repo = 'ta2-minmod-data'  # Replace 'repository-name' with the name of the repository
token = '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'  # Replace 'your_personal_access_token' with your actual PAT


file_content = read_file_from_github(owner, repo, file_path, token)

print(file_content)

if is_json_file_under_data(file_path):
    print(f'{file_path} is a JSON file, running validation on it')
    json_data = {}
    try:
        with open(file_path) as file:
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
    json_string = remove_non_printable_chars(json_string)

    json_data = json.loads(json_string)
    print('Json validated ...')

    if 'MineralSite' in json_data:
        json_data = add_id_to_mineral_site(json_data)
    elif 'MineralSystem' in json_data:
        json_data = add_id_to_mineral_system(json_data)
else:
    print(f'{file_path} is not a JSON file')

