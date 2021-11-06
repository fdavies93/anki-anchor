from sync_types import *
from anki.models import NoteType
from anki.notes import Note
from aqt import mw
from aqt.utils import showInfo, qconnect
from aqt.qt import *
from re import sub

class AnkiWriter(SourceWriter):
    '''Write records to Anki.'''

class AnkiReader(SourceReader):
    '''Read records from Anki.'''
    def __init__(self, parameters : dict):
        self.deck_name = None
        self.note_type_name = None
        if "table" in parameters:
            self.table = parameters["table"] # this is actually the card type
        # if "deck_name" in parameters:
        #     self.deck_name = parameters["deck_name"]

    def get_tables(self) -> list[TableSpec]:
        if mw.col == None:
            return []
        note_types = mw.col.models.all_names_and_ids()
        # decks = mw.col.decks.all_names_and_ids(include_filtered=False)
        return [TableSpec(DATA_SOURCE.ANKI, {id: nt.id}, str(nt.name) ) for nt in note_types]

    def get_columns(self):
        nt : NoteType = mw.col.models.get(self.table.parameters["id"])
        field_names = mw.col.models.fieldNames(nt)
        return [ self._field_to_column(fn) for fn in field_names ]

    def _read_records(self, limit: int = -1, next_iterator = None):
        if self.table == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No table set in AnkiReader; can't read records.")
        note_type_name = self.table.name
        note_ids = mw.col.find_notes(f"note:\"{note_type_name}\"")
        columns = self.get_columns()
        ds = DataSet(columns)
        records = [ self._note_to_record(mw.col.getNote(id)) for id in note_ids ]
        ds.add_records(records)
        return ds

    async def read_records(self, limit : int = -1, next_iterator = None):
        return self._read_records(limit, next_iterator)

    def read_records_sync(self, limit: int = -1, next_iterator=None) -> DataSet:
        return self._read_records(limit, next_iterator)

    # def get_records(self, deck_name, note_type_name: str, columns) -> DataSet:
    #     note_ids = mw.col.find_notes(f"deck:\"{deck_name}\" note:\"{note_type_name}\"")
    #     columns = self.get_columns(note_type_name)
    #     ds = DataSet(columns)
    #     records = [ self._note_to_record(mw.col.getNote(id)) for id in note_ids ]
    #     ds.add_records(records)
    #     return ds

    def _field_to_column(self, fieldName: str):
        if fieldName == "tags": type = COLUMN_TYPE.MULTI_SELECT
        else: type = COLUMN_TYPE.TEXT
        return DataColumn(type, fieldName)

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