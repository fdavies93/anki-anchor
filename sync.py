from enum import Enum
from abc import ABC
from functools import reduce
from typing import Text
import requests

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

class MERGE_TYPE(Enum):
    APPEND = 0
    SOFT_MERGE = 1
    HARD_MERGE = 2

class COLUMN_TYPE(Enum):
    TEXT = 0 # Most Anki fields; Notion tag field
    SELECT = 1 # Notion select field
    MULTI_SELECT = 2 # Notion multi-select / Anki tags field
    DATE = 3 # Notion date field

class SourceReader(ABC):
    '''Read data from some source and turn it into a Python record.'''
    def read_record():
        '''Get a given record.'''

    def read_records():
        '''Get multiple records.'''

    def get_columns():
        '''Get columns with their data type as a standardised type.'''

    def get_databases():
        '''Get database names.'''
    
    def get_record_types():
        '''Get record subtypes. Mostly a workaround for Anki not having "databases" but decks and card types.'''

class SourceWriter(ABC):
    '''Write data from a Python record to some source.'''
    def append_record():
        '''Append one record.'''

    def write_records():
        '''Append multiple records.'''
    
    def amend_record():
        '''Amend record with new fields.'''

class SourceReadWriter(object):
    '''Read and write data from some source to a Python representation of the record.'''
    def __init__(self, reader:SourceReader, writer:SourceWriter) -> None:
        self.reader = reader
        self.writer = writer

class AnkiWriter(SourceWriter):
    '''Write records to Anki.'''

class AnkiReader(SourceReader):
    '''Read records from Anki.'''
    def get_databases():
        pass

    def get_record_types():
        pass

    def get_columns():
        pass

class NotionWriter(SourceWriter):
    '''Write records to Notion.'''


class NotionReader(SourceReader):
    '''Read records from Notion.'''
    def get_databases(self, api_key):
        res = requests.get("https://api.notion.com/v1/databases", auth=BearerAuth(api_key), headers={"Notion-Version": "2021-05-13"})
        json = res.json()
        return [self._parse_database_result(x) for x in json["results"]]

    def get_records(self, api_key: str, id: str, number=100, iterator=None):
        url = f"https://api.notion.com/v1/databases/{id}/query"
        res = requests.post(url, auth=BearerAuth(api_key), headers={"Notion-Version": "2021-05-13"})
        json = res.json()
        return json

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
        type_map = {
            "rich_text": COLUMN_TYPE.TEXT,
            "multi_select": COLUMN_TYPE.MULTI_SELECT,
            "date": COLUMN_TYPE.DATE,
            "created_time": COLUMN_TYPE.DATE
        }
        if column["type"] in type_map:
            col_type = type_map[column["type"]]
        else:
            col_type = COLUMN_TYPE.TEXT
        return {"type": col_type, "name": column_name}
        



def AnkiReadWriter() -> SourceReadWriter:
    return SourceReadWriter(AnkiReader(), AnkiWriter())

def NotionReadWriter() -> SourceReadWriter:
    return SourceReadWriter(NotionReader(), NotionWriter())

class SyncManager(object):
    '''Manages syncs between Notion and Anki.'''
    # Must have options to perform a primary-key merge (overwrite / fill blanks) in addition to simple append
    # Modes
    # -- Append - just add the records from the other source to the current source.
    # -- Soft Merge (must specify merge column) - merge records by merge column, filling any blanks with data from source.
    # -- Hard Merge (must specify merge column)
    #

    def download(self, key: str, database, target_card_type, target_deck, mapping, merge_type=MERGE_TYPE.SOFT_MERGE):
        '''Download records from a given Notion database to Anki.'''
        notion = NotionReadWriter()
        anki = AnkiReadWriter()
        notion.get_columns(database)
        # get database columns
        anki.get_columns(target_deck, target_card_type)
        # get target card type
        if merge_type == MERGE_TYPE.APPEND:
            records = notion.get_all_records()
            # TODO: remap record columns
            anki.append_records(records)
        elif merge_type == MERGE_TYPE.SOFT_MERGE:
            notion_records = notion.get_all_records()
            anki_records = anki.get_all_records()
            
        # for each record
        # ---- create a new anki card
        # ---- fill fields using mapping and source card
        pass

    def upload(self, key: str, database, source_card_type, source_deck, mapping):
        '''Upload records to a given Notion database from Anki.'''
        # get database columns
        # get target card type
        # verify mappings between target_card_type and target_deck
        # if mappings are good, then:
        # -- for each card:
        # ---- create a new record
        # ---- fill fields using mapping and source card
        pass

    def get_anki_fields(self, card_type):

        pass

    def get_notion_fields(self):
        pass

    def list_databases(self, key):
        # notion search api with filter
        pass

    def list_database_columns(self, key, database):
        pass

sync = SyncManager()