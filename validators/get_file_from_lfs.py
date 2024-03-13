import os
import subprocess
import requests
import sys

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

    response = requests.post(url, json=data, headers=headers)

    print(response.status_code)
    print(response.json())

    response_json = response.json()

    download_url = response_json['objects'][0]['actions']['download']['href']

    # Making the API call to fetch the file content
    response = requests.get(download_url)

    # Assigning the response text to the variable 'file_content'
    file_content = response.text

    # Printing the first 10 lines of the output text
    print('\n'.join(file_content.split('\n')[:10]))

    return file_content


def get_lfs_objects(file_content, branch):
    metadata = file_content.split('\n')
    oid_line = next((line for line in metadata if line.startswith("oid")), None)
    size_line = next((line for line in metadata if line.startswith("size")), None)
    print(size_line)
    if oid_line:
        oid = oid_line.split(" ")[-1][7:]
        size = size_line.split(" ")[-1]
        print(f"Old SHA: {oid}")
        print(f"size {size}")
        file_content = download_file_with_git_lfs(oid, int(size), branch)
        return file_content
    else:
        print('Invalid response ', file_content)
        sys.exit(1)
