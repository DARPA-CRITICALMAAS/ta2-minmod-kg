REPO_URL="https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data"

url="https://api.github.com/repos/DARPA-CRITICALMAAS/ta2-minmod-data/commits/main"

url_code="https://api.github.com/repos/DARPA-CRITICALMAAS/ta2-minmod-kg/commits/main"

# Run curl command and capture the response
bearer_token="a_token"
accept_header="application/vnd.github+json"
github_version="2022-11-28"

date_value=$(curl -s "Authorization: Bearer $bearer_token" -H "Accept: $accept_header" "X-GitHub-Api-Version: $github_version" "$url" | jq -r '.commit.author.date')

# Get the current time in UTC
current_utc_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Print or use the UTC time as needed

# Convert date_value and current_utc_time to Unix timestamps
# date_value_timestamp=$(date -d "$date_value" -u +"%s")
current_utc_timestamp=$(date -u -d "$current_utc_time" +"%s")

echo "Commit time in UTC: $date_value"

echo "Current time in UTC: $current_utc_time"

# Convert date_value to seconds since epoch
date_value_timestamp=$(date -u -d "$date_value" "+%s")


echo "Commit time in UTC timestamp: $date_value_timestamp"


echo "Current time in UTC timestamp: $current_utc_timestamp"

# Calculate the difference in seconds
time_difference=$((current_utc_timestamp - date_value_timestamp))



echo $time_difference

# Check if the time difference is greater than 1 hour
# Change this before deployment
if [ "$time_difference" -gt 720 ]; then
    echo "date_value is older than 2 hour(s) compared to current UTC time."
    # exit
else
    echo "date_value is within the last 2 hour(s) compared to current UTC time."
fi



# Define the local directory where you want to pull the changes
python3.8 -m venv venv

source venv/bin/activate

pip install rdflib || echo "Module already installed"

pip install requests || echo "Module already installed"

pip install pyshacl || echo "Module already installed"

pip install jsonschema || echo "Module already installed"

pip install drepr || echo "Module already installed"

pip install validators || echo "validators already installed"


# Define the local directory where you want to pull the changes
TOMCAT_DIR="/var/local/mindmod/apache-tomcat-7.0.109/"

TARGET_FOLDER="/var/local/mindmod/LodviewDemo/target/lodview.war"

LOCAL_DIR_DATA="/var/local/mindmod/ta2-minmod-data/"

LOCAL_DIR_CODE="/var/local/mindmod/ta2-minmod-kg/"

LOCAL_DIR="/var/local/mindmod/ta2-minmod-data/"

DATA_DIR="/var/local/mindmod/generated_data/"

DATA="data/"
ENTITIES="entities/"
COMMODITIES="commodities/"
DEPOSIT="depositTypes/"
UNITS="units/"
SAMEAS="sameAs/"
UMN="umn"
SRI="sri"
USC="usc/"
INFERLINK="inferlink/"
VALIDATORS="validators/"
GENERATOR="generator/"
TTL_FILES="ttl_files/"
JSON_VALIDATOR="generate_file_with_id.py"
TTL_VALIDATOR="validate_pyshacl_on_file.py"
TTL_MODEL_FILE="model.yml"
DEPLOYMENT="deployment/"
MERGE_JSON="merge_jsons.py"
MERGE_JSON_FOLDER="json_files/"
EXTRACTIONS="extractions"
MERGEDTTL="merged_ttl/"

COMMODITIES_FILE_YML="model_commodities.yml"
COMMODITIES_FILE_CSV="minmod_commodities.csv"
COMMODITIES_FILE_TTL="minmod_commodities.ttl"

DEPOSITS_FILE_YML="model_deposits.yml"
DEPOSITS_FILE_CSV="minmod_deposit_types.csv"
DEPOSITS_FILE_TTL="minmod_deposits.ttl"

UNITS_FILE_YML="model_units.yml"
UNITS_FILE_CSV="minmod_units.csv"
UNITS_FILE_TTL="minmod_units.ttl"

SAMEAS_FILE_YML="same_as.yml"
SAMEAS_FILE_CSV="sameas_mineralsites.csv"
SAMEAS_FILE_TTL="same_as.ttl"
# Change to the local directory
cd $LOCAL_DIR_DATA

git checkout mineral-system-validation
# Perform a Git pull from the repository
git fetch origin

sleep 2s

git reset --hard HEAD

git clean -fd

git merge main

sleep 2s


# Change to the local directory
cd $LOCAL_DIR_CODE

git checkout transferring-code-new-repo
# Perform a Git pull from the repository
git fetch origin

sleep 2s

git reset --hard HEAD

git clean -fd

git merge main

sleep 2s

#Commodities
python -m drepr -r $LOCAL_DIR_DATA$DATA$ENTITIES$COMMODITIES$COMMODITIES_FILE_YML -d default=$LOCAL_DIR_DATA$DATA$ENTITIES$COMMODITIES$COMMODITIES_FILE_CSV -o $LOCAL_DIR_DATA$DATA$ENTITIES$COMMODITIES$COMMODITIES_FILE_TTL

#Deposit Types
python -m drepr -r $LOCAL_DIR_DATA$DATA$ENTITIES$DEPOSIT$DEPOSITS_FILE_YML -d default=$LOCAL_DIR_DATA$DATA$ENTITIES$DEPOSIT$DEPOSITS_FILE_CSV -o $LOCAL_DIR_DATA$DATA$ENTITIES$DEPOSIT$DEPOSITS_FILE_TTL

#Units
python -m drepr -r $LOCAL_DIR_DATA$DATA$ENTITIES$UNITS$UNITS_FILE_YML -d default=$LOCAL_DIR_DATA$DATA$ENTITIES$UNITS$UNITS_FILE_CSV -o $LOCAL_DIR_DATA$DATA$ENTITIES$UNITS$UNITS_FILE_TTL


#Same AS
python -m drepr -r $LOCAL_DIR_DATA$DATA$ENTITIES$SAMEAS$SAMEAS_FILE_YML -d default=$LOCAL_DIR_DATA$DATA$ENTITIES$SAMEAS$SAMEAS_FILE_CSV -o $LOCAL_DIR_DATA$DATA$ENTITIES$SAMEAS$SAMEAS_FILE_TTL


#Validate json in all files in inferlink folder

# Path to the folder containing files
folder_path_inferlink="$LOCAL_DIR_DATA$DATA$INFERLINK$EXTRACTIONS"
folder_path_umn="$LOCAL_DIR_DATA$DATA$UMN"

# Path to the Python script you want to run on each file
json_script_path="$LOCAL_DIR_CODE$VALIDATORS$JSON_VALIDATOR"

json_files_path="$DATA_DIR$MERGE_JSON_FOLDER"
folder_path_inferlink_id="$json_files_path$INFERLINK"
folder_path_sri="$LOCAL_DIR_DATA$DATA$SRI"
folder_path_sri_mc="$LOCAL_DIR_DATA$DATA$SRI""/""mappableCriteria"



echo $folder_path_inferlink
echo $folder_path_inferlink_id


echo $json_script_path

#First delete any existing file

find $folder_path_inferlink_id -type f -exec rm {} \;

for file_path in "$folder_path_inferlink"/*; do
    # Check if the item is a file (not a directory)
    if [ -f "$file_path" ]; then
        # Run the Python script on the current file
        echo $file_path
        python3 "$json_script_path" "$file_path" "$folder_path_inferlink_id"
        if [ $? -ne 0 ]; then
            echo "Python script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done


# Validate SRI as well

folder_path_sri="/var/local/mindmod/ta2-minmod-data/data/sri"
folder_path_sri_id="$json_files_path$SRI"
echo $folder_path_sri_id

#First delete any existing file

find $folder_path_sri_id -type f -exec rm {} \;

echo $folder_path_sri

for file_path in "$folder_path_sri"/*; do
    # Check if the item is a file (not a directory)
    if [ -f "$file_path" ]; then
        echo $file_path
        # Run the Python script on the current file
        python "$json_script_path" "$file_path" "$folder_path_sri_id"
        if [ $? -ne 0 ]; then
            echo "Validate json script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done

# Validate UMN as well

folder_path_umn_id="$json_files_path$UMN"
echo $folder_path_umn_id

#First delete any existing file

find $folder_path_umn_id -type f -exec rm {} \;

echo $folder_path_umn


for file_path in "$folder_path_umn"/*; do
    # Check if the item is a file (not a directory)
    if [ -f "$file_path" ]; then
        echo $file_path
        # Run the Python script on the current file
        python3 "$json_script_path" "$file_path" "$folder_path_umn_id"
        if [ $? -ne 0 ]; then
            echo "Validate json script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done

# Create ttl file from all files in inferlink folder
drepr_yaml_path="$LOCAL_DIR_CODE""generator/$TTL_MODEL_FILE"

drepr_yaml_path_mc="$LOCAL_DIR_CODE""generator/""model_mineral_system.yml"

save_ttl_files_path="$DATA_DIR$TTL_FILES$INFERLINK"

echo $drepr_yaml_path

echo $save_ttl_files_path

echo $folder_path_inferlink_id

#First delete any existing file

find $save_ttl_files_path -type f -exec rm {} \;


for file_path in "$folder_path_inferlink_id"/*; do
    # Check if the item is a file (not a directory)
    if [ -f "$file_path" ]; then
        # Run the Python script on the current file
        filename=$(basename "$file_path")
        echo $filename
        filename_no_ext="${filename%.*}"

        echo $filename_no_ext
        generated_ttl_path="$save_ttl_files_path$filename_no_ext"".ttl"

        echo $generated_ttl_path

        # Echo the command

        drepr_command='python -m drepr -r "$drepr_yaml_path" -d default="$file_path" -o "$generated_ttl_path"'

        echo "Running command: python -m drepr -r \"$drepr_yaml_path\" -d default=\"$file_path\" -o \"$generated_ttl_path\""

        echo "Running command: $drepr_command"

        # Run the command
        eval "$drepr_command"

        if [ $? -ne 0 ]; then
            echo "Python script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done

# Create ttl file from all files in sri folder

save_ttl_files_path_sri="$DATA_DIR$TTL_FILES$SRI""/"

#First delete any existing file

find $save_ttl_files_path_sri -type f -exec rm {} \;

for file_path in "$folder_path_sri_id"/*; do
    # Check if the item is a file (not a directory)
    if [ -f "$file_path" ]; then
        # Run the Python script on the current file
        filename=$(basename "$file_path")
        echo $filename
        filename_no_ext="${filename%.*}"

        echo $filename_no_ext
        generated_ttl_path="$save_ttl_files_path_sri$filename_no_ext"".ttl"

        echo $generated_ttl_path

        # Echo the command

        drepr_command='python -m drepr -r "$drepr_yaml_path" -d default="$file_path" -o "$generated_ttl_path"'

        echo "Running command: $drepr_command"

        # Run the command
        eval "$drepr_command"

        if [ $? -ne 0 ]; then
            echo "Python script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done

# Create ttl file from all files in umn folder

save_ttl_files_path_umn="$DATA_DIR$TTL_FILES$UMN""/"

echo $folder_path_umn_id
#First delete any existing file

find $save_ttl_files_path_umn -type f -exec rm {} \;

for file_path in "$folder_path_umn_id"/*; do
    # Check if the item is a file (not a directory)
    if [ -f "$file_path" ]; then
        # Run the Python script on the current file
        filename=$(basename "$file_path")
        echo $filename
        filename_no_ext="${filename%.*}"

        echo $filename_no_ext
        generated_ttl_path="$save_ttl_files_path_umn$filename_no_ext"".ttl"

        echo $generated_ttl_path

        # Echo the command

        drepr_command='python -m drepr -r "$drepr_yaml_path" -d default="$file_path" -o "$generated_ttl_path"'

        echo "Running command: $drepr_command"

        echo "Running command: python -m drepr -r \"$drepr_yaml_path\" -d default=\"$file_path\" -o \"$generated_ttl_path\""
        # Run the command
        eval "$drepr_command"

        if [ $? -ne 0 ]; then
            echo "Python script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done

# Create ttl file from all files in umn folder

save_ttl_files_path_usc="$LOCAL_DIR_DATA$DATA$USC"

# Merge all inferlink files

merged_file_inferlink="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_inferlink.ttl"

merge_ttl_script="$LOCAL_DIR_CODE$GENERATOR""merge_ttls_in_folder.py"

> "$merged_file_inferlink"

python3 "$merge_ttl_script" "$save_ttl_files_path" "$merged_file_inferlink"

# Merge all umn files

echo $save_ttl_files_path_umn

merged_file_umn="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_umn.ttl"

echo $merged_file_umn

> "$merged_file_umn"

python3 "$merge_ttl_script" "$save_ttl_files_path_umn" "$merged_file_umn"

# Merge all sri files

echo $save_ttl_files_path_sri

merged_file_sri="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_sri.ttl"

echo $merged_file_sri

> "$merged_file_sri"

python "$merge_ttl_script" "$save_ttl_files_path_sri" "$merged_file_sri"



# Merge all usc files

merged_file_usc="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_usc.ttl"

> "$merged_file_usc"

python3 "$merge_ttl_script" "$save_ttl_files_path_usc" "$merged_file_usc"


all_ttl_files="/var/local/mindmod/ttl_data/"

final_file="$DATA_DIR$TTL_FILES""final_0229.ttl"
existing_ttl_file="$all_ttl_files""final.ttl"
> "$final_file"

echo $final_file

echo $existing_ttl_file
cat "$existing_ttl_file" >> "$final_file"

echo $LOCAL_DIR_DATA$DATA$ENTITIES$COMMODITIES$COMMODITIES_FILE_TTL
cat "$LOCAL_DIR_DATA$DATA$ENTITIES$COMMODITIES$COMMODITIES_FILE_TTL" >> "$final_file"

echo $LOCAL_DIR_DATA$DATA$ENTITIES$DEPOSIT$DEPOSITS_FILE_TTL
cat "$LOCAL_DIR_DATA$DATA$ENTITIES$DEPOSIT$DEPOSITS_FILE_TTL" >> "$final_file"

echo $LOCAL_DIR_DATA$DATA$ENTITIES$UNITS$UNITS_FILE_TTL
cat "$LOCAL_DIR_DATA$DATA$ENTITIES$UNITS$UNITS_FILE_TTL" >> "$final_file"

echo $LOCAL_DIR_DATA$DATA$ENTITIES$SAMEAS$SAMEAS_FILE_TTL
cat "$LOCAL_DIR_DATA$DATA$ENTITIES$SAMEAS$SAMEAS_FILE_TTL" >> "$final_file"


echo $merged_file_umn
cat "$merged_file_umn" >> "$final_file"

echo $merged_file_usc
cat "$merged_file_usc" >> "$final_file"

echo $merged_file_inferlink
cat "$merged_file_inferlink" >> "$final_file"

echo $merged_file_sri
cat "$merged_file_sri" >> "$final_file"

deactivate

python3.11 -m venv venv-v2

source venv-v2/bin/activate

pip3 install rdflib || echo "Module already installed"

pip3 install requests || echo "Module already installed"

pip3 install pyshacl || echo "Module already installed"

pip3 install jsonschema || echo "Module already installed"

pip3 install drepr-v2 || echo "Module already installed"

pip3 install validators || echo "validators already installed"

# Validate SRI MC as well


folder_path_sri_mc_id="$json_files_path$SRI""/""mappableCriteria"
echo $folder_path_sri_mc_id

#First delete any existing file

find $folder_path_sri_mc_id -type f -exec rm {} \;

echo $folder_path_sri_mc

for file_path in "$folder_path_sri_mc"/*; do
    # Check if the item is a file (not a directory)
    if [ -f "$file_path" ]; then
        echo $file_path
        # Run the Python script on the current file
        python "$json_script_path" "$file_path" "$folder_path_sri_mc_id"
        if [ $? -ne 0 ]; then
            echo "Validate json script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done

echo $drepr_yaml_path_mc



# Create ttl file from all files in sri mc folder

save_ttl_files_path_sri_mc="$DATA_DIR$TTL_FILES$SRI""/""mappableCriteria"

#First delete any existing file

find $save_ttl_files_path_sri_mc -type f -exec rm {} \;

for file_path in "$folder_path_sri_mc_id"/*; do
    # Check if the item is a file (not a directory)
    if [ -f "$file_path" ]; then
        # Run the Python script on the current file
        filename=$(basename "$file_path")
        echo $filename
        filename_no_ext="${filename%.*}"

        echo $filename_no_ext
        generated_ttl_path="$save_ttl_files_path_sri""/mappableCriteria/""$filename_no_ext"".ttl"

        echo $generated_ttl_path

        # Echo the command
        # python -m drepr model.yml default=default.json --progfile write_to_str.py --debuginfo > default.ttl

        drepr_command='python -m drepr "$drepr_yaml_path_mc" default="$file_path" > "$generated_ttl_path"'

        echo "Running command: $drepr_command"

        # Run the command
        eval "$drepr_command"

        if [ $? -ne 0 ]; then
            echo "Python script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done


# Merge all sri MC files

echo $save_ttl_files_path_sri_mc

merged_file_sri_mc="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_sri_mc.ttl"

echo $merged_file_sri_mc

> "$merged_file_sri_mc"

python "$merge_ttl_script" "$save_ttl_files_path_sri_mc" "$merged_file_sri_mc"

echo $merged_file_sri_mc
cat "$merged_file_sri_mc" >> "$final_file"

cp $final_file $all_ttl_files

deactivate