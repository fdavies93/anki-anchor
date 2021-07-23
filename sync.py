from enum import Enum
from abc import ABC
from functools import reduce
from typing import Text
from anki.cards import Card
from anki.models import NoteType
from anki.notes import Note
import requests
from dataclasses import dataclass, field
from aqt import mw
from aqt.utils import showInfo, qconnect
from aqt.qt import *
from re import sub

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

class MERGE_TYPE(Enum):
    APPEND = 0
    APPEND_NO_DUPLICATES = 1
    SOFT_MERGE = 2
    HARD_MERGE = 3

class COLUMN_TYPE(Enum):
    TEXT = 0 # Most Anki fields; Notion tag field
    SELECT = 1 # Notion select field
    MULTI_SELECT = 2 # Notion multi-select / Anki tags field
    DATE = 3 # Notion date field

@dataclass
class RecordReadDataType:
    columns: list
    records: list
    iterator: str

@dataclass
class Column:
    type: COLUMN_TYPE
    name: str

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
    def get_databases(self):
        '''Returns a list of note types and decks.'''
        note_types = mw.col.models.all_names_and_ids()
        decks = mw.col.decks.all_names_and_ids(include_filtered=False)
        return {"note_types": note_types, "decks": decks}

    def get_columns(self, note_type: NoteType):
        nt = mw.col.models.get(note_type["id"])
        field_names = mw.col.models.fieldNames(nt)
        return [ self._field_to_column(fn) for fn in field_names ]

    def get_records(self, deck_name, note_type_name: str, columns):
        note_ids = mw.col.find_notes(f"deck:\"{deck_name}\" note:\"{note_type_name}\"")
        records = [ self._note_to_record(mw.col.getNote(id)) for id in note_ids ]
        # return RecordReadDataType(columns, records)
        return records

    def _field_to_column(self, fieldName: str):
        if fieldName == "tags": type = COLUMN_TYPE.MULTI_SELECT
        else: type = COLUMN_TYPE.TEXT
        return Column(type, fieldName)

    def _note_to_record(self, note: Note):
        out_dict = {}
        for k, v in note.items():
            if k != "tags":
                out_dict[k] = self._remove_html_basic(v)
        out_dict["tags"] = note.tags
        return out_dict

    def _remove_html_basic(self, string: str):
        # This is probably a bit hacky, but should be ok.
        # Proper HTML handling requires an XML library.
        return sub("<[^>]*>", "", string)

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

    def get_records(self, api_key: str, id: str, column_info: dict, number=100, iterator=None) -> RecordReadDataType:
        url = f"https://api.notion.com/v1/databases/{id}/query"
        data = {"page_size": number}
        if iterator is not None: data["start_cursor"] = iterator
        res = requests.post(url, json=data, auth=BearerAuth(api_key), headers={"Notion-Version": "2021-05-13"})
        json = res.json()
        it = json["next_cursor"]
        records = [ self._map_record(record, column_info) for record in json["results"] ]
        return RecordReadDataType(column_info, records, it)

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


def AnkiReadWriter() -> SourceReadWriter:
    return SourceReadWriter(AnkiReader(), AnkiWriter())

def NotionReadWriter() -> SourceReadWriter:
    return SourceReadWriter(NotionReader(), NotionWriter())

class SyncManager(object):
    '''Manages syncs between Notion and Anki.'''
    def __init__(self) -> None:
        self.notion_reader = NotionReader()
        self.anki_reader = AnkiReader()
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

    def get_anki_card_types(self):
        ar = AnkiReader()
        return ar.get_databases()

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