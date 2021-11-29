from sync_types import *
from os.path import *
from typing import Callable
import json

class JsonWriter(SourceWriter):
    ''' Write records to a JSON file. '''
    # records are basically written as-is, though date columns are transformed to text
    # header is added to list out columns and types
    # {
    #   header: [ "COLUMN_NAME": COLUMN_TYPE ]
    #   records: [ RECORD ...]
    # }
    def __init__(self, table_spec : TableSpec):
        if "file_path" not in table_spec.parameters:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No path parameter found when initialising JsonWriter.")
        self.path = join(dirname(realpath(__file__)), table_spec.parameters["file_path"])

    def _check_path_set(self):
        if self.path == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "Path not set in JsonWriter.")

    def _create_table(self, dataset: DataSet, callback : Callable = None):
        ''' Sync function which is basis for async and sync methods. '''
        clone = copy.deepcopy(dataset)

        column_dict = {}

        date_columns = []
        for col in clone.columns:
            column_dict[col.name] = int(col.type)
            if col.type == COLUMN_TYPE.DATE:
                date_columns.append(col.name)
        for date_col in date_columns:
            clone.change_column_type(date_col, COLUMN_TYPE.TEXT)
        
        format_as_dict = { "multiselect_delimiter": clone.format.multiselect_delimiter, "time_format": clone.format.time_format }

        records_list = [ r.asdict() for r in clone.records ]
        
        json_obj = {
            "header": {
                "columns": column_dict,
                "format": format_as_dict
            },
            "records": records_list
        }

        try:
            with open(self.path, 'w', encoding="utf-8") as f:
                json.dump(json_obj, f, indent=4)
        except:
            raise SyncError(SYNC_ERROR_CODE.FILE_ERROR)

        return callback
    
    async def create_table(self, dataset: DataSet, callback : Callable = None):
        return self._create_table(dataset, callback)

    def create_table_sync(self, dataset: DataSet, callback : Callable = None):
        return self._create_table(dataset, callback)

    # async def append_records(self, limit : int = -1, next_iterator = None, callback : Callable = None):
    #     # convert records to a writable format
        
    #     try:
    #         with open(self.path, 'w', encoding="utf-8") as f:
    #             pass
    #     except:
    #         raise SyncError(SYNC_ERROR_CODE.FILE_ERROR)

class JsonReader(SourceReader):
    ''' Read records from a JSON file. '''
    def __init__(self, table_spec : TableSpec):
        if "file_path" not in table_spec.parameters:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No path parameter found when initialising JsonReader.")
        self.path = join(dirname(realpath(__file__)), table_spec.parameters["file_path"])
    
    async def read_records(self, limit : int = -1, next_iterator = None) -> DataSet:
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