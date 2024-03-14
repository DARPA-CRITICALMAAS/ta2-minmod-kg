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
    print('Pulling data from batch API')
    response = requests.post(url, json=data, headers=headers)
    print('Pulled data from batch API')
    response_json = response.json()

    download_url = response_json['objects'][0]['actions']['download']['href']
    response = requests.get(download_url)
    file_content = response.text

    return file_content


def get_lfs_objects(file_content, branch):
    metadata = file_content.split('\n')
    oid_line = next((line for line in metadata if line.startswith("oid")), None)
    size_line = next((line for line in metadata if line.startswith("size")), None)
    if oid_line:
        oid = oid_line.split(" ")[-1][7:]
        size = size_line.split(" ")[-1]
        file_content = download_file_with_git_lfs(oid, int(size), branch)
        return file_content
    else:
        print('Invalid response ', file_content)
        sys.exit(1)
