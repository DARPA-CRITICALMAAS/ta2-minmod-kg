#!/bin/bash

REPO_URL="https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data"

url="https://api.github.com/repos/DARPA-CRITICALMAAS/ta2-minmod-data/commits/main"

url_code="https://api.github.com/repos/DARPA-CRITICALMAAS/ta2-minmod-kg/commits/main"

# Run curl command and capture the response
bearer_token="a_bearer_token"
accept_header="application/vnd.github+json"
github_version="2022-11-28"

date_value=$(curl -s "Authorization: Bearer $bearer_token" -H "Accept: $accept_header" "X-GitHub-Api-Version: $github_version" "$url" | jq -r '.commit.author.date')

# Get the current time in UTC
current_utc_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Print or use the UTC time as needed
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
threshold=$((2*3600 + 10*60))
# Check if the time difference is greater than 3 hour
# Change this before deployment
if [ $"$time_difference" -gt $threshold ]; then
    echo "date_value is older than 2 hour(s) compared to current UTC time."
    exit
else
    echo "date_value is within the last 2 hour(s) compared to current UTC time."
fi



# Define the local directory where you want to pull the changes

MINMOD="/var/local/mindmod/"


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
TTL_MODEL_FILE="model_mineral_site.yml"
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

git reset --hard HEAD

git clean -fd

git pull

git checkout main

git fetch origin

git merge main

sleep 2s


cd $LOCAL_DIR_CODE

git reset --hard HEAD

git clean -fd

git pull

git checkout main

git fetch origin

git merge main

# Define the local directory where you want to pull the changes
python3.11 -m venv venv

source venv/bin/activate

pip3 install rdflib || echo "Module already installed"

pip3 install requests || echo "Module already installed"

pip3 install pyshacl || echo "Module already installed"

pip3 install jsonschema || echo "Module already installed"

git clone https://github.com/binh-vu/drepr-v2.git
pip install drepr-v2/

pip3 install validators || echo "module already installed"

pip3 install python-slugify || echo "module already installed"

pip install typer==0.9.0 || echo "module already installed"


#Commodities
python -m drepr $LOCAL_DIR_CODE$GENERATOR$ENTITIES$COMMODITIES$COMMODITIES_FILE_YML default=$LOCAL_DIR_DATA$DATA$ENTITIES$COMMODITIES$COMMODITIES_FILE_CSV > $DATA_DIR$DATA$ENTITIES$COMMODITIES$COMMODITIES_FILE_TTL

#Deposit Types
python -m drepr $LOCAL_DIR_CODE$GENERATOR$ENTITIES$DEPOSIT$DEPOSITS_FILE_YML default=$LOCAL_DIR_DATA$DATA$ENTITIES$DEPOSIT$DEPOSITS_FILE_CSV > $DATA_DIR$DATA$ENTITIES$DEPOSIT$DEPOSITS_FILE_TTL

#Units
python -m drepr $LOCAL_DIR_CODE$GENERATOR$ENTITIES$UNITS$UNITS_FILE_YML default=$LOCAL_DIR_DATA$DATA$ENTITIES$UNITS$UNITS_FILE_CSV > $DATA_DIR$DATA$ENTITIES$UNITS$UNITS_FILE_TTL

#First delete any existing file
all_ttl_files="/var/local/mindmod/ttl_data/"
final_file="$DATA_DIR$TTL_FILES""final_temp.ttl"
existing_ttl_file="$all_ttl_files""final.ttl"
> "$final_file"
echo $final_file

folder_path_same_as=$LOCAL_DIR_DATA$DATA$UMN"/sameas/"

# Iterate over each file in the folder ending with '_sameas.csv'
for file in "$folder_path_same_as"/*_sameas.csv; do
    # Check if the file exists
    if [ -f "$file" ]; then

        echo "Running command on file: $file"
        filename=$(basename "$file")
        echo $filename
        filename_no_ext="${filename%.*}"
        echo $filename_no_ext
        generated_ttl_path="$DATA_DIR$DATA$ENTITIES$SAMEAS$filename_no_ext"".ttl"
        echo $generated_ttl_path
        python -m drepr $LOCAL_DIR_CODE$GENERATOR$ENTITIES$SAMEAS$SAMEAS_FILE_YML default=$file > $generated_ttl_path
        cat "$generated_ttl_path" >> "$final_file"

    fi
done

#Validate json in all files in inferlink folder

# Path to the folder containing files
folder_path_inferlink="$LOCAL_DIR_DATA$DATA$INFERLINK$EXTRACTIONS"
folder_path_umn="$LOCAL_DIR_DATA$DATA$UMN"
json_script_path="$LOCAL_DIR_CODE$VALIDATORS$JSON_VALIDATOR"
json_files_path="$DATA_DIR$MERGE_JSON_FOLDER"
folder_path_inferlink_id="$json_files_path$INFERLINK"

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
        python "$json_script_path" "$file_path" "$folder_path_inferlink_id"
        if [ $? -ne 0 ]; then
            echo "Python script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done

# Validate SRI as well

folder_path_sri="$LOCAL_DIR_DATA$DATA$SRI"
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

# # Validate UMN as well

folder_path_umn_id="$json_files_path$UMN"
echo $folder_path_umn_id
find $folder_path_umn_id -type f -exec rm {} \;
echo $folder_path_umn

for file_path in "$folder_path_umn"/*; do
    # Check if the item is a file (not a directory)
    if [ -f "$file_path" ]; then
        echo $file_path
        # Run the Python script on the current file
        python "$json_script_path" "$file_path" "$folder_path_umn_id"
        if [ $? -ne 0 ]; then
            echo "Validate json script failed"
            # Replace by say an email or some call to inform someone
        fi
    fi
done

# Create ttl file from all files in inferlink folder
drepr_yaml_path="$LOCAL_DIR_CODE""generator/$TTL_MODEL_FILE"
drepr_yaml_path_mc="$LOCAL_DIR_CODE""generator/""model_mineral_system_v2.yml"



save_ttl_files_path="$DATA_DIR$TTL_FILES$INFERLINK"
echo $drepr_yaml_path
echo $save_ttl_files_path
echo $folder_path_inferlink_id
find $save_ttl_files_path -type f -exec rm {} \;
file_list_inferlink=""
for file_path in "$folder_path_inferlink_id"/*; do
    if [ -f "$file_path" ]; then
        filename=$(basename "$file_path")
        echo $filename
        filename_no_ext="${filename%.*}"
        echo $filename_no_ext
        generated_ttl_path="$save_ttl_files_path$filename_no_ext"".ttl"
        echo $generated_ttl_path
        drepr_command='python3 -m drepr "$drepr_yaml_path" default="$file_path"'
        echo "Running command: $drepr_command"
        eval "$drepr_command" > "$generated_ttl_path"

            file_list_inferlink="$file_list_inferlink $generated_ttl_path"
        if [ $? -ne 0 ]; then
            echo "Python script failed"
        fi
    fi
done


# Create ttl file from all files in sri folder

save_ttl_files_path_sri="$DATA_DIR$TTL_FILES$SRI""/"
find $save_ttl_files_path_sri -type f -exec rm {} \;
file_list_sri=""
for file_path in "$folder_path_sri_id"/*; do
    if [ -f "$file_path" ]; then
        filename=$(basename "$file_path")
        echo $filename
        filename_no_ext="${filename%.*}"
        echo $filename_no_ext
        generated_ttl_path="$save_ttl_files_path_sri$filename_no_ext"".ttl"
        echo $generated_ttl_path
        drepr_command='python3 -m drepr "$drepr_yaml_path" default="$file_path"'
        echo "Running command: $drepr_command"
         eval "$drepr_command" > "$generated_ttl_path"
            file_list_sri="$file_list_sri "$generated_ttl_path""
        if [ $? -ne 0 ]; then
            echo "Python script failed"
        fi
    fi
done

# Create ttl file from all files in umn folder

save_ttl_files_path_umn="$DATA_DIR$TTL_FILES$UMN""/"
echo $folder_path_umn_id
find $save_ttl_files_path_umn -type f -exec rm {} \;
file_list_umn=""
for file_path in "$folder_path_umn_id"/*; do
    if [ -f "$file_path" ]; then
        filename=$(basename "$file_path")
        echo $filename
        filename_no_ext="${filename%.*}"
        echo $filename_no_ext
        generated_ttl_path="$save_ttl_files_path_umn$filename_no_ext"".ttl"
        echo $generated_ttl_path
        drepr_command='python3 -m drepr "$drepr_yaml_path" default="$file_path"'
        echo "Running command: $drepr_command"
         eval "$drepr_command" > "$generated_ttl_path"
            file_list_umn="$file_list_umn $generated_ttl_path"
        if [ $? -ne 0 ]; then
            echo "Python script failed"
        fi
    fi
done



# Create ttl file from all files in usc folder

save_ttl_files_path_usc="$LOCAL_DIR_DATA$DATA$USC"
file_list_usc=""
for file in "$save_ttl_files_path_usc"/*; do
    if [ -f "$file" ]; then
        file_list_usc="$file_list_usc $file"
    fi
done

cd /var/local/mindmod/apache-jena-5.0.0/bin/

# Merge all inferlink files
echo $file_list_inferlink
merged_file_inferlink="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_inferlink.ttl"
merge_ttl_script="$LOCAL_DIR_CODE$GENERATOR""merge_ttls_in_folder.py"
> "$merged_file_inferlink"
./riot  --syntax=ttl --output=ttl --union $file_list_inferlink >> "$merged_file_inferlink"

# Merge all umn files
echo $save_ttl_files_path_umn
merged_file_umn="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_umn.ttl"
echo $merged_file_umn
> "$merged_file_umn"
/var/local/mindmod/apache-jena-5.0.0/bin/riot  --syntax=ttl --output=ttl --union $file_list_umn >> "$merged_file_umn"


# # Merge all sri files
echo $save_ttl_files_path_sri
merged_file_sri="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_sri.ttl"
echo $merged_file_sri
> "$merged_file_sri"
/var/local/mindmod/apache-jena-5.0.0/bin/riot  --syntax=ttl --output=ttl --union $file_list_sri >> "$merged_file_sri"


# Merge all usc files
echo "$file_list_usc"
merged_file_usc="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_usc.ttl"
> "$merged_file_usc"
./riot  --syntax=ttl --output=ttl --union $file_list_usc >> "$merged_file_usc"



# Define the local directory where you want to pull the changes

echo $merged_file_umn
echo $merged_file_usc
echo $merged_file_inferlink
echo $merged_file_sri


# Validate SRI MC as well

folder_path_sri_mc="$LOCAL_DIR_DATA$DATA$SRI""/""mappableCriteria"
folder_path_sri_mc_id="$json_files_path$SRI""/""mappableCriteria"
echo $folder_path_sri_mc_id

# #First delete any existing file

find $folder_path_sri_mc_id -type f -exec rm {} \;

echo $folder_path_sri_mc
file_list_sri_mc=""

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
find $save_ttl_files_path_sri_mc -type f -exec rm {} \;
file_list_sri_mc=""
for file_path in "$folder_path_sri_mc_id"/*; do
    if [ -f "$file_path" ]; then
        filename=$(basename "$file_path")
        echo $filename
        filename_no_ext="${filename%.*}"
        echo $filename_no_ext
        generated_ttl_path="$save_ttl_files_path_sri""/mappableCriteria/""$filename_no_ext"".ttl"
        echo $generated_ttl_path
        drepr_command='python3 -m drepr "$drepr_yaml_path_mc" default="$file_path"'
        echo "Running command: $drepr_command"
        eval "$drepr_command" > "$generated_ttl_path"
            file_list_sri_mc="$file_list_sri_mc $generated_ttl_path"
        if [ $? -ne 0 ]; then
            echo "Python script failed"
        fi
    fi
done




# Merge all sri MC files

echo $save_ttl_files_path_sri_mc
merged_file_sri_mc="$DATA_DIR$TTL_FILES$MERGEDTTL""merged_file_sri_mc.ttl"
echo $merged_file_sri_mc
> "$merged_file_sri_mc"
/var/local/mindmod/apache-jena-5.0.0/bin/riot  --syntax=ttl --output=ttl --union $file_list_sri_mc >> "$merged_file_sri_mc"

sleep 2s


echo $existing_ttl_file
# cat "$existing_ttl_file" >> "$final_file"
echo $DATA_DIR$DATA$ENTITIES$COMMODITIES$COMMODITIES_FILE_TTL
cat "$DATA_DIR$DATA$ENTITIES$COMMODITIES$COMMODITIES_FILE_TTL" >> "$final_file"

echo $DATA_DIR$DATA$ENTITIES$DEPOSIT$DEPOSITS_FILE_TTL
cat "$DATA_DIR$DATA$ENTITIES$DEPOSIT$DEPOSITS_FILE_TTL" >> "$final_file"

echo $DATA_DIR$DATA$ENTITIES$UNITS$UNITS_FILE_TTL
cat "$DATA_DIR$DATA$ENTITIES$UNITS$UNITS_FILE_TTL" >> "$final_file"


same_as_csv_path=$LOCAL_DIR_DATA$DATA$UMN"/sameas/"

# Check if the folder exists
if [ -d "$same_as_csv_path" ]; then
    # Iterate over files ending with '_sameas.csv'
    for file in "$same_as_csv_path"/*_sameas.csv; do
        # Check if the file exists
        if [ -f "$file" ]; then
           python -m drepr $LOCAL_DIR_CODE$GENERATOR$ENTITIES$SAMEAS$SAMEAS_FILE_YML default=$file  >> "$final_file"

        fi
    done
else
    echo "Folder not found: $same_as_csv_path"
fi

/var/local/mindmod/apache-jena-5.0.0/bin/riot --syntax=ttl --output=ttl --union  "$merged_file_usc" "$merged_file_umn" "$merged_file_sri"  "$merged_file_inferlink" "$merged_file_sri_mc"  >> "$final_file"

cp $final_file $all_ttl_files

deactivate

cd /var/local/mindmod/apache-jena-fuseki-4.9.0
fuser -k 3030/tcp
./fuseki-server --file "$final_file"  /minmod
