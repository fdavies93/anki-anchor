from enum import IntEnum
from abc import ABC

import requests

from dataset import *

class DATA_SOURCE(IntEnum):
    JSON = 0,
    TSV = 1,
    ANKI = 2,
    NOTION = 3

class SYNC_ERROR_CODE(IntEnum):
    PARAMETER_NOT_FOUND = 0,
    FILE_ERROR = 1,
    INCORRECT_SOURCE = 2

@dataclass
class TableSpec:
    source : DATA_SOURCE
    parameters : dict # contains specific information for a given data source
    name: str

@dataclass
class SyncHandle:
    ''' Maintains position in a read or write operation. Returned from read and write operations. '''
    records: DataSet
    source: DATA_SOURCE
    handle: object = None # always starts as a None object; later declared

    def close(self):
        ''' Closes the handle. To be implemented by derived objects.'''

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

    async def read_records(self, limit : int = -1, next_iterator : SyncHandle = None):
        ''' Get multiple records. '''
    
    def read_records_sync(self, limit: int = -1, next_iterator : SyncHandle = None):
        ''' Get records synchronously. '''

    async def get_columns(self):
        '''Get columns with their data type as a standardised type.'''

    async def get_tables(self):
        '''Get a list of tables. In Anki's case this is a list of card_types and how they intersect with decks.'''
    
    async def get_record_types(self):
        '''Get record subtypes. Mostly a workaround for Anki not having "databases" but decks and card types.'''

class SourceWriter(ABC):
    '''Write data from a Python record to some source.'''

    async def create_table(self, dataset: DataSet, callback):
        ''' Create a table (or file) based on the spec of the dataset. '''

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