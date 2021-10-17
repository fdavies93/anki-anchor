from sync import *
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
    def get_databases(self):
        '''Returns a list of note types and decks.'''
        note_types = mw.col.models.all_names_and_ids()
        decks = mw.col.decks.all_names_and_ids(include_filtered=False)
        return {"note_types": note_types, "decks": decks}

    def get_columns(self, note_type: NoteType):
        nt = mw.col.models.get(note_type["id"])
        field_names = mw.col.models.fieldNames(nt)
        return [ self._field_to_column(fn) for fn in field_names ]

    def get_records(self, deck_name, note_type_name: str, columns) -> DataSet:
        note_ids = mw.col.find_notes(f"deck:\"{deck_name}\" note:\"{note_type_name}\"")
        # note type MUST be consistent for 
        records = [ self._note_to_record(mw.col.getNote(id)) for id in note_ids ]
        # return RecordReadDataType(columns, records)
        return records

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