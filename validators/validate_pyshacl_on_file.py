import sys
import validate_pyshacl

# Temporaryily needed till all the files in main don't follow the schema

filename = sys.argv[1]
print(filename)
validate_mineral_site = sys.argv[2]

try:
    with open(filename, 'r') as file:
        data_graph = file.read()
except FileNotFoundError:
    print(f"Error: File '{filename}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")

if validate_mineral_site == 1:
    validate_pyshacl.validate_using_shacl_mineral_site(data_graph)
else:
    validate_pyshacl.validate_mineral_system_using_shacl(data_graph)
