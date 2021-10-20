from enum import Enum
from abc import ABC
from os import read
from typing import Text

import requests

from dataset import *
import json
from os.path import dirname, exists, join, realpath
import asyncio

class SYNC_ERROR_CODE(Enum):
    PARAMETER_NOT_FOUND = 0,
    FILE_ERROR = 1

class SyncError(ValueError):
    def __init__(self, error_code: SYNC_ERROR_CODE, message="Sync error."):
        self.error_code = error_code
        super().__init__(message)

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

class SourceReader(ABC):
    '''Read data from some source and turn it into a Python record.'''
    async def read_record(self):
        '''Get a given record.'''

    async def read_records(self, limit : int = -1, next_iterator = None):
        '''Get multiple records.'''

    async def get_columns(self):
        '''Get columns with their data type as a standardised type.'''

    async def get_tables(self):
        '''Get a list of tables. In Anki's case this is a list of card_types and how they intersect with decks.'''
    
    async def get_record_types(self):
        '''Get record subtypes. Mostly a workaround for Anki not having "databases" but decks and card types.'''

class SourceWriter(ABC):
    '''Write data from a Python record to some source.'''
    async def append_record(self, record: DataRecord, dataset: DataSet, callback):
        '''Append one record asynchronously.'''

    async def append_records(self, records: list, dataset: DataSet, callback):
        '''Append multiple records asynchronously.'''
        
    async def amend_record(self, key_col: str, record: DataRecord, dataset: DataSet):
        '''Amend record with new fields asynchronously.'''

    async def amend_records(self, key_col: str, record: DataRecord, dataset: DataSet):
        '''Amend records in source with new values from dataset, using key_col to join the two tables.'''

    def append_record_sync(self, record: DataRecord, dataset: DataSet):
        '''Append one record synchronously.'''
    
    def append_records_sync(self, records: list, dataset: DataSet):
        '''Append multiple records synchronously.'''

    def amend_record_sync(self, key_val, key_col: str, record: DataRecord, dataset: DataSet):
        '''Amend record synchronously.'''


class SourceReadWriter(object):
    '''Read and write data from some source to a Python representation of the record.'''
    def __init__(self, reader:SourceReader, writer:SourceWriter) -> None:
        self.reader = reader
        self.writer = writer

class JsonWriter(SourceWriter):
    ''' Write records to a JSON file. '''
    # records are basically written as-is, though date columns are transformed to text
    # header is added to list out columns and types
    # {
    #   header: [ "COLUMN_NAME": COLUMN_TYPE ]
    #   records: [ RECORD ...]
    # }

class JsonReader(SourceReader):
    ''' Read records from a JSON file. '''
    def __init__(self, parameters : dict):
        if "file_path" not in parameters:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No path parameter found when initialising JsonReader.")
        self.path = join(dirname(realpath(__file__)), parameters["file_path"])
    
    async def read_records(self, limit : int = -1, next_iterator = None):
        # open file
        try:
            with open(self.path, 'r', encoding="utf-8") as f:
                raw_json = json.load(f)
        except:
            raise SyncError(SYNC_ERROR_CODE.FILE_ERROR, "Error reading file in.")

        format_raw = raw_json["header"]["format"]
        format_obj = DataSetFormat( multiselect_delimiter= format_raw["multiselect_delimiter"], time_format=format_raw["time_format"] )

        columns_raw : dict = raw_json["header"]["columns"]
        
        date_cols = []

        for col in columns_raw:
            if COLUMN_TYPE(columns_raw[col]) == COLUMN_TYPE.DATE:
                columns_raw[col] = COLUMN_TYPE.TEXT
                date_cols.append(col)

        columns_obj = [ DataColumn(COLUMN_TYPE(columns_raw[col]), col) for col in columns_raw ]

        ds = DataSet(columns_obj, records=raw_json["records"], format=format_obj)
        
        for date_col in date_cols:
            ds.change_column_type(date_col, COLUMN_TYPE.DATE) # reformat all dates in the file to actually be datetime objects

        return ds
        # print (raw_json)

class TsvWriter(SourceWriter):
    ''' Write records to a TSV file. '''

class TsvReader(SourceReader): 
    ''' Read records from a TSV file. '''



class NotionWriter(SourceWriter):
    '''Write records to Notion.'''


class NotionReader(SourceReader):
    '''Read records from Notion.'''
    def __init__(self) -> None:
        self.type_map = {
            "title": COLUMN_TYPE.TEXT,
            "rich_text": COLUMN_TYPE.TEXT,
            "multi_select": COLUMN_TYPE.MULTI_SELECT,
            "date": COLUMN_TYPE.DATE,
            "created_time": COLUMN_TYPE.DATE
        }
        self.read_strategies = {
            'title': self._map_notion_text,
            'rich_text': self._map_notion_text,
            'multi_select': self._map_notion_multiselect,
            'date': self._map_notion_date,
            'created_time': self._map_notion_created_time
        }

    def get_databases(self, api_key):
        data = { "filter": {"property": "object", "value": "database"} }
        res = requests.post("https://api.notion.com/v1/search", json=data, auth=BearerAuth(api_key), headers={"Notion-Version": "2021-05-13"})
        json = res.json()
        return [self._parse_database_result(x) for x in json["results"]]

    def get_records(self, api_key: str, id: str, column_info: dict, number=100, iterator=None) -> DataSet:
        url = f"https://api.notion.com/v1/databases/{id}/query"
        data = {"page_size": number}
        if iterator is not None: data["start_cursor"] = iterator
        res = requests.post(url, json=data, auth=BearerAuth(api_key), headers={"Notion-Version": "2021-05-13"})
        json = res.json()
        it = json["next_cursor"]
        records = [ self._map_record(record, column_info) for record in json["results"] ]
        return DataSet(column_info, records, it)

    def get_record_types(self):
        pass

    def get_columns(self, api_key, id):
        url = f"https://api.notion.com/v1/databases/{id}"
        res = requests.get(url, auth=BearerAuth(api_key), headers={"Notion-Version": "2021-05-13"})
        json = res.json()
        return [ self._map_column(k,json["properties"][k]) for k in json["properties"] ]

    def _parse_database_result(self, result):
        return {'name': result["title"][0]["text"]["content"], 'id': result["id"]}

    def _map_column(self, column_name, column):
        if column["type"] in self.type_map:
            col_type = self.type_map[column["type"]]
        else:
            col_type = COLUMN_TYPE.TEXT
        return {"type": col_type, "name": column_name}
    
    def _map_record(self, record:dict, columns:list):
        out_dict = {}
        column_names = [ col['name'] for col in columns ]
        for k in column_names:
            if k in record["properties"]:
                v = record["properties"][k]
                prop_type = v["type"]
                if prop_type in self.read_strategies:
                    # print (f"{prop_type} is in read strategies.")
                    out_dict[k] = self.read_strategies[prop_type](v)
                else: out_dict[k] = None # If we don't know how to handle the type, just return null.
            else:
                out_dict[k] = None
        return out_dict

    def _map_notion_text(self, prop):
        # Database text properties might be of different types (title etc), so 
        # we get the type before looking for the content; database properties only
        # ever contain a single block, so we're safe to look at index [0]
        type = prop["type"] 
        if (len(prop[type]) > 0):
            return prop[type][0]["plain_text"]
        else: return ""

    def _map_notion_date(self, prop):
        return prop["date"]

    def _map_notion_multiselect(self, prop):
        return [ r["name"] for r in prop["multi_select"] ]

    def _map_notion_created_time(self, prop):
        return prop["created_time"]


# def AnkiReadWriter() -> SourceReadWriter:
#     return SourceReadWriter(AnkiReader(), AnkiWriter())

# def NotionReadWriter() -> SourceReadWriter:
#     return SourceReadWriter(NotionReader(), NotionWriter())

# class SyncManager(object):
#     '''Manages syncs between Notion and Anki.'''
#     def __init__(self) -> None:
#         self.notion_reader = NotionReader()
#         self.anki_reader = AnkiReader()
#     # Must have options to perform a primary-key merge (overwrite / fill blanks) in addition to simple append
#     # Modes
#     # -- Append - just add the records from the other source to the current source.
#     # -- Soft Merge (must specify merge column) - merge records by merge column, filling any blanks with data from source.
#     # -- Hard Merge (must specify merge column)
#     #

#     def download(self, key: str, database, target_card_type, target_deck, mapping, merge_type=MERGE_TYPE.SOFT_MERGE):
#         '''Download records from a given Notion database to Anki.'''
#         notion = NotionReadWriter()
#         anki = AnkiReadWriter()
#         notion.get_columns(database)
#         # get database columns
#         anki.get_columns(target_deck, target_card_type)
#         # get target card type
#         if merge_type == MERGE_TYPE.APPEND:
#             records = notion.get_all_records()
#             # TODO: remap record columns
#             anki.append_records(records)
#         elif merge_type == MERGE_TYPE.SOFT_MERGE:
#             notion_records = notion.get_all_records()
#             anki_records = anki.get_all_records()
            
#         # for each record
#         # ---- create a new anki card
#         # ---- fill fields using mapping and source card
#         pass

#     def upload(self, key: str, database, source_card_type, source_deck, mapping):
#         '''Upload records to a given Notion database from Anki.'''
#         # get database columns
#         # get target card type
#         # verify mappings between target_card_type and target_deck
#         # if mappings are good, then:
#         # -- for each card:
#         # ---- create a new record
#         # ---- fill fields using mapping and source card
#         pass

#     def get_anki_card_types(self):
#         ar = AnkiReader()
#         return ar.get_databases()

#     def get_anki_fields(self, card_type):

#         pass

#     def get_notion_fields(self):
#         pass

#     def list_databases(self, key):
#         # notion search api with filter
#         pass

#     def list_database_columns(self, key, database):
#         pass

# sync = SyncManager()