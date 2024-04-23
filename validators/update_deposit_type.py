import subprocess
import json
import validate_pyshacl
import validator_utils
import json
from rdflib import Graph, Namespace, URIRef, Literal

def run_drepr_on_file(datasource, model_file):
    destination = 'generated_files/ttl_files/'
    command = f' python -m drepr {model_file} default="{datasource}"'
    print('Running ... ', command)

    try:
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
        output_lines = result.stdout.splitlines()[2:]  # Skip the first two lines
        output_data = '\n'.join(output_lines)
        return output_data
    except subprocess.CalledProcessError as e:
        print("Error executing command:", e)
        print("Command output (if any):", e.output)
        return ''

def create_deposit(mineral_site_id, id, confidence, normalized_uri, source, observed_name):
    dp_new = {
        "normalized_uri": normalized_uri,
        "source": source,
        "confidence": confidence,
        "observed_name": observed_name
    }
    dep_new_id = validator_utils.deposit_uri((dp_new), mineral_site_id)
    data = {
        "MineralSite": [
            {
                "id": mineral_site_id,
                "deposit_type_candidate": [
                    {
                        "id": id,
                        "endDate": "2024-04-22"                       
                    },
                    {
                        "id": dep_new_id,
                        "normalized_uri": normalized_uri,
                        "source": source,
                        "confidence": confidence,
                        "observed_name": observed_name
                    }
                ]
            }
        ]
    }
    json_data = data

    # Specify the file path
    file_path = "data.json"
    
    # Open the file in write mode
    with open(file_path, "w") as json_file:
        # Write the JSON object to the file
        json.dump(data, json_file)

    print(run_drepr_on_file(file_path, "/Users/namratasharma/Documents/USC/OnCampus/DARPA/ta2-minmod-kg/generator/deposit_type_model.yml"))

        # Create an RDF graph
    g = Graph()
    
    # Define namespaces
    ns = Namespace("https://minmod.isi.edu/resource/")
    g.bind("ex", ns)
    
    # Convert JSON data to RDF triples
    subject = URIRef("https://minmod.isi.edu/resource/")
    for key, value in json_data.items():
        predicate = URIRef(f"https://minmod.isi.edu/resource/{key}")
        g.add((subject, predicate, Literal(value)))
    
    # Serialize RDF graph to Turtle format
    ttl_data = g.serialize(format="turtle")
    
    # Print TTL data
    print(ttl_data)

create_deposit("mineral_sitede09e33898e8b0c9d2fc8bfe9817ce399c9f3b3b5389c3cfe86218913b06b0ff", "deposit_type00c6fc519fa5ebf0d7ade74524bd20f56d2ec7407bb440fd2529856d416d24d1", 1.0, "https://djfjsd/", "source", "my name")


    
