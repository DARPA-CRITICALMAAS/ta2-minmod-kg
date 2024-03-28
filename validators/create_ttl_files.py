import subprocess
import validate_pyshacl
import validator_utils

def run_drepr_on_file(datasource, model_file, base_path, temp_file):
    destination = base_path + 'generated_files/ttl_files/'
    command = f' python -m drepr -r {model_file} -d default="{datasource}"'
    print('Running ... ', command)

    try:
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
        output_lines = result.stdout.splitlines()[2:]  # Skip the first two lines
        output_data = '\n'.join(output_lines)
        with open(temp_file, 'w') as file:
            file.write(output_data)
        # clean_content = validator_utils.remove_non_printable_chars(output_data)
        print('Generated ttl ...')
        return output_data
    except subprocess.CalledProcessError as e:
        print("Error executing command:", e)
        print("Command output (if any):", e.output)
        return ''


def run_drepr_on_mineral_site(datasource, base_path, temp_file):
    model_file = base_path + 'generator/model.yml'
    print(model_file)
    return run_drepr_on_file(datasource, model_file, base_path, temp_file)


def run_drepr_on_mineral_system(datasource, base_path):
    model_file = base_path + 'generator/model_mineral_system.yml'
    return run_drepr_on_file(datasource, model_file, base_path)

def remove_non_printable_chars(text):
    # Remove vertical tabs and newlines
    clean_text = text.replace('\n', ' ').replace('\\u000b', '').replace('\\n', ' ').replace('\\"', ' ').replace('\"', ' ')
    return clean_text


def create_drepr_file_mineral_site(file_path, base_path, temp_file):
    print(base_path)
    file_content = run_drepr_on_mineral_site(file_path, base_path, temp_file)
    # validated_drepr = validate_pyshacl.validate_using_shacl_mineral_site(temp_file)

    if False:
        print('Pyshacl Validation failed for Mineral Site')
        raise
    else:
        print('Pyshacl Validation succeeded Mineral Site')


def create_drepr_file_mineral_system(file_path, base_path):
    file_content = run_drepr_on_mineral_system(file_path, base_path)
    validated_drepr = validate_pyshacl.validate_mineral_system_using_shacl(file_content)

    if not validated_drepr:
        print('Pyshacl Validation failed for pyshacl Mineral System')
        raise
    else:
        print('Pyshacl Validation succeeded Mineral System')


def create_drepr_from_mineral_site(file_path, base_path, temp_file):
    create_drepr_file_mineral_site(file_path, base_path, temp_file)


def create_drepr_from_mineral_system(file_path, base_path):
    create_drepr_file_mineral_system(file_path, base_path)
