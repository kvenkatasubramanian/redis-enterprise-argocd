
import sys
import os
dbname = os.getenv('DB_NAME')

data = [
    {
        "uid": 1,
        "name": "test",
        "data_persistence": "disabled"
    },
    {
        "uid": 2,
        "name": "test2",
        "data_persistence": "disabled"
    }
]

db_info = {}
for item in data:
    uid = item['uid']
    name = item['name']
    persistence = item['data_persistence']
    # If dbname is set, skip non-matching DBs
    if dbname and name != dbname and persistence == "aof":
        continue

    db_info[name] = {'uid': uid, 'no_of_keys': 1}
    db_info[name]["persistence"] = (item["data_persistence"] == "aof")

print(db_info)
if dbname and dbname not in db_info:
    print(f"Database '{dbname}' does not exist in the cluster.")


for db_name, db_details in db_info.items():
    persistence = db_details['persistence']   
    if persistence:
        uid = db_details['uid'] 
        # print(db_info)