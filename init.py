from requests import get, post, put, delete
from requests.auth import HTTPBasicAuth

import sys
import getopt
import time


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


opts, args = getopt.getopt(sys.argv[1:], "hu:p:a:i:x", [
                           "user", "pass", "address", "indicies"])
params = dict(opts)

basic = HTTPBasicAuth(params['-u'], params['-p'])
headers = {
    "Content-Type": "application/json"
}
es_host = f"https://{params['-a']}:9200"

indices_wildcard = "*" if '-i' not in params else params['-i']

repo_rsp = get(f"{es_host}/_snapshot/az_repo", auth=basic, verify=False)
if repo_rsp.status_code != 200:
    log("Snapshot repository not found. Creating it...")

    repo_body = {
        "type": "azure",
        "settings": {
            "container": "snapshots",
            "readonly": True,
        }
    }

    repo_rsp = put(f"{es_host}/_snapshot/az_repo",
                   auth=basic, verify=False, headers=headers, json=repo_body)

    if repo_rsp.status_code != 200:
        raise Exception("Failed to create repository")

restore_body = {
    "indices": f"{indices_wildcard},-.*",
    "ignore_index_settings": "index.search*",
    "rename_pattern": "(.+)",
    "rename_replacement": "restored-$1"
}

log(f"Restoring snapshot for {indices_wildcard}...")

restore_rsp = post(f"{es_host}/_snapshot/az_repo/snapshot_1/_restore?wait_for_completion=false",
                   auth=basic, verify=False, headers=headers, json=restore_body)

if restore_rsp.status_code != 200:
    raise Exception(
        f"Failed to restore snapshot. Received status code {restore_rsp.status_code} and body {restore_rsp.json()}")

while True:
    recovery = get(f"{es_host}/_cat/recovery?active_only",
                   auth=basic, verify=False)
    if recovery.text == "":
        break
    log("Restoring is in progress... Recovery response: ", recovery.text)
    time.sleep(2)

log("Restore completed")

indices_resp = get(f"{es_host}/restored-*", auth=basic, verify=False)
if indices_resp.status_code != 200:
    raise Exception("Failed to get indices list")

indices_payload = indices_resp.json()

bad_fields = ["creation_date", "provided_name", "uuid", "version", "blocks"]
# each key in the dict is an index name, each value is the index settings/mappings etc
for index_name, payload in indices_payload.items():
    if index_name[0] == '.':
        continue

    # remove stuff from payload that break the put index request
    for field in bad_fields:
        if field in payload["settings"]["index"]:
            del payload["settings"]["index"][field]

    new_index_name = index_name.replace('restored-', '')

    log(f"Creating index {new_index_name}...")

    put_index_resp = put(f"{es_host}/{new_index_name}",
                         auth=basic, verify=False, headers=headers, json=payload)
    if put_index_resp.status_code != 200:
        log(
            f"Failed to create index {new_index_name}. Received status code {put_index_resp.status_code} and body {put_index_resp.json()}")
        continue

    log(f"Waiting for cluster to be green...")

    health_resp = get(f"{es_host}/_cluster/health?wait_for_status=green&timeout=50s",
                      auth=basic, verify=False)
    if health_resp.status_code != 200:
        log(
            f"Failed to wait for cluster health. Received status code {health_resp.status_code} and body {health_resp.json()}")
        continue

    reindex_body = {
        "source": {"index": index_name},
        "dest": {"index": new_index_name}
    }

    log(f"Reindexing {index_name} to {new_index_name}...")

    reindex_resp = post(f"{es_host}/_reindex?wait_for_completion=false",
                        auth=basic, verify=False, headers=headers, json=reindex_body)
    reindex_resp_json = reindex_resp.json()

    if reindex_resp.status_code != 200:
        log(
            f"Failed to reindex {index_name} to {new_index_name}. Received status code {reindex_resp.status_code} and body {reindex_resp.json()}")
        continue

    task_id = reindex_resp_json["task"]

    while True:
        task_status_resp = get(
            f"{es_host}/_tasks/{task_id}", auth=basic, verify=False)
        task_status_resp_json = task_status_resp.json()
        if task_status_resp_json["completed"]:
            break
        log(
            f"Reindexing is in progress... Task status: {task_status_resp_json}")
        time.sleep(10)

    if task_status_resp_json.get("error", False):
        log(
            f"Reindexing failed. Received status code {task_status_resp.status_code} and body {task_status_resp_json}")
        continue

    log("Reindex completed. Deleting source index...")

    delete_resp = delete(f"{es_host}/{index_name}", auth=basic, verify=False)
    if delete_resp.status_code != 200:
        log(f"Failed to delete {index_name}")
        continue
