# from .sync import *
from json import dump, dumps, load
import json
import requests
from sync import *

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

def test_notion_read(config):
    nr = NotionReader()
    database_info = nr.get_databases(config["notion_key"])
    print(json.dumps(database_info, indent=4))

def test_notion_get_columns(config):
    nr = NotionReader()
    dbs = nr.get_databases(config["notion_key"])
    myId = dbs[0]["id"]
    columns_info = nr.get_columns(config["notion_key"],myId)
    print(columns_info)

def test_notion_get_records(config):
    nr = NotionReader()
    dbs = nr.get_databases(config["notion_key"])
    myId = dbs[0]["id"]
    column_info = nr.get_columns(config["notion_key"], myId)
    cur_records = nr.get_records(config["notion_key"], myId, column_info)
    record_set = []
    record_set.extend(cur_records.records)
    it = 1
    while (cur_records.iterator is not None):
        # print (f"Going for run {it}. Iterator is {cur_records.iterator}")
        cur_records = nr.get_records(config["notion_key"], myId, column_info, iterator=cur_records.iterator)
        record_set.extend(cur_records.records)
        it += 1
    print(json.dumps(record_set, indent=4))

def main():
    fh = open("./config.json", "r")
    config = load(fh)
    # test_notion_read(config)
    # test_notion_get_columns(config)
    test_notion_get_records(config)

if __name__ == "__main__":
    main()