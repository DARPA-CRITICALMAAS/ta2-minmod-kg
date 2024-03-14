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
import git

def is_valid_json(s):
    try:
        json.loads(s)
        return True
    except ValueError:
        return False

def get_lfs_objects(file_path, branch):
    repo_url = 'https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data.git'

    # Clone the repository to a temporary directory
    repo_dir = 'temp_repo'
    repo = git.Repo.clone_from(repo_url, repo_dir)

    # Navigate to the repository directory
    os.chdir(repo_dir)

    # Download the file using git-lfs
    repo.git.checkout(branch)

    # Read the content of the file
    with open(file_path, 'r') as file:
        content = file.read()

    # Print or process the content as needed
    print('\n'.join(content.split('\n')[:10]))

    # Clean up: delete the temporary repository directory
    os.chdir('..')
    os.system(f'rm -rf {repo_dir}')
    return content

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
    print(response_json)

    download_url = response_json['objects'][0]['actions']['download']['href']

    print(download_url)
    response_data = requests.get(download_url)
    # file_content = response.text

    if response_data.status_code == 200:
        return ''
    else:
        print('Not 200 ')
        return ''

def read_file_from_github(owner, repo, path_to_file, token, branch):
    url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path_to_file}"
    headers = {"Authorization": f"token {token}"}
    print('Making a call to github')
    response = requests.get(url, headers=headers)

    file_info = ''
    print('Made a call to github')

    if response.status_code == 200:
        file_info = response.text
        if not is_valid_json(file_info):
            file_info = get_lfs_objects(path_to_file , branch)

    elif response.status_code == 404:
        print(f"File '{file_path}' was deleted, skipping.")
        sys.exit(0)
    else:
        print(f"Failed to read file. Status code: {response.status_code}")
        sys.exit(1)

    return file_info

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


def add_id_to_mineral_site(json_data, file_path):
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
    create_ttl_files.create_drepr_from_mineral_site(file_path)



def add_id_to_mineral_system(json_data, file_path):
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
    create_ttl_files.create_drepr_from_mineral_system(file_path)


changed_files = sys.argv[1]
temp_file = sys.argv[2]
token = sys.argv[3]
branch = sys.argv[4]
file_path = changed_files

owner = 'DARPA-CRITICALMAAS'
repo = 'ta2-minmod-data'

print(changed_files, branch)

if validator_utils.is_json_file_under_data(file_path):
    print(f'{file_path} is a JSON file, running validation on it')
    file_content = read_file_from_github(owner, repo, file_path, token, branch)

    # print(f'{file_path} is a JSON file, running validation on it')
    json_data = {}
    # file_content = validator_utils.remove_non_printable_chars(file_content)
    print('*************')
    try:
        json_data = json.loads(file_content)
        if 'MineralSite' in json_data:
            print('Mineral Site validation ...')
            json_data = validate_json_schema(json_data)
        elif 'MineralSystem' in json_data:
            print('Mineral System validation ...')
            json_data = validate_json_schema_mineral_system(json_data)
        else:
            print('No validation', json_data)
    except FileNotFoundError:
        print(f"File '{file_path}' was deleted, skipping.")
        sys.exit(0)
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    json_string = (file_content)
    print('------------')
    # print(json_string)

    json_data = json.loads(json_string)
    print('Json validated ...')

    if 'MineralSite' in json_data:
        json_data = add_id_to_mineral_site(json_data, temp_file)
    elif 'MineralSystem' in json_data:
        json_data = add_id_to_mineral_system(json_data, temp_file)
else:
    print(f'{file_path} is not a JSON file')

