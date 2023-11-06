from requests import get, post, put, delete
from requests.auth import HTTPBasicAuth

import sys, getopt

opts, args = getopt.getopt(sys.argv[1:],"hu:p:a:i:x",["user","pass","address","indicies"])
params = dict(opts)

basic = HTTPBasicAuth(params['-u'], params['-p'])
headers = {
"Content-Type": "application/json"
}
es_host = f"https://{params['-a']}:9200"
print(es_host)
indices_wildcard = "*" if '-i' not in params else params['-i']

print(indices_wildcard)

############################
# restoring indices from 
############################

restore_bodey = {
                    "indices": f"{indices_wildcard},-.*",
                    "ignore_index_settings": "index.search*",
                    "rename_pattern": "(.+)",
                    "rename_replacement": "restored-$1"
                }
restore_rsp = post(f"{es_host}/_snapshot/az_repo/snapshot_1/_restore?wait_for_completion=false", auth=basic, verify=False, headers=headers, json=restore_bodey)
print(restore_rsp.request.url)
print(restore_rsp.request.body)
print(restore_rsp.request.headers)
res = restore_rsp.json()
print(res)
# # Get all indices + settings
# indices_resp = get(f"{es_host}/{indices_wildcard}", auth=basic, verify=False)

# if indices_resp.status_code != 200:
#     raise Exception("Failed to get indices list")

# indices_payload = indices_resp.json()

# bad_fields = ["creation_date", "provided_name", "uuid", "version", "blocks"]
# # each key in the dict is an index name, each value is the index settings/mappings etc
# for index_name, payload in indices_payload.items():
#     if index_name[0] == '.':
#         continue

#     # remove stuff from payload that break the put index request
#     for field in bad_fields:
#         if field in payload["settings"]["index"]:
#             del payload["settings"]["index"][field]


#     # Create the source index
#     new_index_name = index_name.replace('restored-','')
#     put_index_resp = put(f"{es_host}/{new_index_name}", auth=basic, verify=False, headers=headers, json=payload)

#     if put_index_resp.status_code != 200:
#         print(f"Failed to create indicex {index_name}")
#         continue
    
#     # Reindex with suffix
#     reindex_payload = {"source": {"index": index_name}, "dest": {"index": new_index_name}}
    
#     # TODO: this is very likely to time out on medium/large indices. You need to add the query param wait_for_completion=false and add a loop that checks if the task id is really done
#     reindex_resp = post(f"{es_host}/_reindex", auth=basic, verify=False, headers=headers, json=reindex_payload)

#     if reindex_resp.status_code != 200:
#         print(f"Failed to reindex {index_name} to {new_index_name}")
#         continue

#     # Delete source index
#     delete_resp = delete(f"{es_host}/{index_name}", auth=basic, verify=False)
#     if delete_resp.status_code != 200:
#         print(f"Failed to reindex {index_name} to {new_index_name}")
#         continue