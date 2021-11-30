from collections import deque
from typing import Deque
from anki import collection
from anki.decks import DeckManager
from sync_types import *
from anki.models import *
from anki.notes import *
from aqt import mw
from aqt.utils import showInfo, qconnect
from aqt.qt import *
from re import sub

@dataclass
class AnkiSyncHandle(SyncHandle):

    def __init_subclass__(cls) -> None:
        return super().__init_subclass__()

    def __enter__(self):
        self.it = 0
        self.id_list = []
        # No setup required right now.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class AnkiWriter(SourceWriter):
    '''Write records to Anki.'''
    def __init__(self, parameters : dict):
        if mw.col == None:
            mw.loadCollection()
        self.table = None

    def set_table(self, table: TableSpec):
        if table.source != DATA_SOURCE.ANKI:
            raise SyncError(SYNC_ERROR_CODE.INCORRECT_SOURCE)
        else:
            self.table = table


    def _make_template(self, fields = list):
        # actually makes a side of a card, in language users understand
        temp = mw.col.models.new_template("front")
        temp["qfmt"] = "".join(["{{",fields[0],"}}"])
        return temp

    def create_table(self, dataset: DataSet, name: str, callback = None):
        new_model = mw.col.models.new(name)
        fields = []
        for col in dataset.columns:
            fields.append( mw.col.models.new_field(col.name) )
        new_model["flds"] = fields
        # this is properly part of the UI layer, so here contains only a placeholder
        new_model["tmpls"] = [self._make_template(dataset.column_names)]
        changes = mw.col.models.add_dict(new_model)
        new_id = mw.col.models.id_for_name(name)
        return TableSpec(DATA_SOURCE.ANKI, {"id": int(new_id)}, name)

    def _write_records(self, dataset : DataSet, limit: int = -1, next_iterator : AnkiSyncHandle = None):
        # just writes to the default deck for now, filtering on note_type (as that's the actual schema)
        type_clean = {
            COLUMN_TYPE.SELECT: COLUMN_TYPE.TEXT,
            COLUMN_TYPE.DATE: COLUMN_TYPE.TEXT,
            COLUMN_TYPE.MULTI_SELECT: COLUMN_TYPE.TEXT
        }
        
        if next_iterator != None:
            remaining_records = copy.copy(next_iterator.handle)
        else:
            remaining_records = deque()
            safe_data : DataSet = dataset.make_write_safe(type_clean).op_returns["safe_data"]
            for record in safe_data.records:
                remaining_records.append(record.asdict())

        note_type = mw.col.models.get(self.table.parameters["id"])
        target_deck = int(mw.col.decks.all_names_and_ids()[0].id)

        cur_it = 0

        while len(remaining_records) > 0 and cur_it != limit:
            cur_it += 1
            record = remaining_records.popleft()
            new_note = mw.col.new_note(note_type)
            for field in record:
                new_note[field] = str(record[field]) # prevents Nones from causing issues
            mw.col.add_note(new_note, target_deck)
        
        if len(remaining_records) == 0:
            remaining_records = None
            
        out_it = AnkiSyncHandle(source = DATA_SOURCE.ANKI, records = dataset, handle = remaining_records)

        return out_it

    async def write_records(self, dataset : DataSet, limit : int = -1, next_iterator : AnkiSyncHandle = None):
        return self._write_records(dataset, limit, next_iterator)

    def write_records_sync(self, dataset : DataSet, limit : int = -1, next_iterator : AnkiSyncHandle = None):
        return self._write_records(dataset, limit, next_iterator)

class AnkiReader(SourceReader):
    '''Read records from Anki.'''
    def __init__(self, parameters : dict):
        if mw.col == None:
            mw.loadCollection()
        self.deck_name = None
        self.note_type_name = None
        self.table = None
        if "table" in parameters:
            self.table = parameters["table"] # this is actually the card type
        # if "deck_name" in parameters:
        #     self.deck_name = parameters["deck_name"]

    def set_table(self, table: TableSpec):
        if table.source != DATA_SOURCE.ANKI:
            raise SyncError(SYNC_ERROR_CODE.INCORRECT_SOURCE)
        else:
            self.table = table

    def get_decks(self) -> list:
        return mw.col.decks.all_names_and_ids()

    def get_tables(self) -> list[TableSpec]:
        if mw.col == None:
            return []
        note_types = mw.col.models.all_names_and_ids()
        # decks = mw.col.decks.all_names_and_ids(include_filtered=False)
        return [TableSpec(DATA_SOURCE.ANKI, {"id": nt.id}, str(nt.name) ) for nt in note_types]

    def get_columns(self):
        nt : NoteType = mw.col.models.get(self.table.parameters["id"])
        field_names = mw.col.models.fieldNames(nt)
        return [ self._field_to_column(fn) for fn in field_names ]

    def _read_records(self, limit: int = -1, next_iterator : SyncHandle = None):
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