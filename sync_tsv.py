from sync_types import *
from os.path import *
from typing import Callable
import csv

class TsvWriter(SourceWriter):
    ''' Write records to a TSV file. '''
    def __init__(self, table_spec : TableSpec = None):
        self.path = None
        if "file_path" in table_spec.parameters:
            self.path = join(dirname(realpath(__file__)), table_spec.parameters["file_path"])
    
    def _check_path_set(self):
        if self.path == None:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "Path not set in TsvWriter.")

    def _create_table(self, dataset:DataSet, callback : Callable = None):
        self._check_path_set()

        conversions = {
            COLUMN_TYPE.DATE: COLUMN_TYPE.TEXT,
            COLUMN_TYPE.MULTI_SELECT: COLUMN_TYPE.TEXT,
            COLUMN_TYPE.SELECT: COLUMN_TYPE.TEXT
        }

        safe_ds = dataset.make_write_safe(conversions).op_returns["safe_data"]
        record_list = [r.asdict() for r in safe_ds.records]

        try:
            with open(self.path, 'w', encoding="utf-8") as f:
                writer = csv.DictWriter(f, dataset.column_names, delimiter="\t")
                writer.writeheader()
                for r in record_list:
                    writer.writerow(r)
        except:
            raise SyncError(SYNC_ERROR_CODE.FILE_ERROR)

        return callback

    async def create_table(self, dataset: DataSet, callback : Callable = None):
        ''' Write a TSV file from given dataset. This will overwrite any existing file at the given file path. '''
        return self._create_table(dataset, callback)
    
    def create_table_sync(self, dataset: DataSet, callback : Callable = None):
        return self._create_table(dataset, callback)

class TsvReader(SourceReader): 
    ''' Read records from a TSV file. '''
    def __init__(self, table_spec : dict):
        if "file_path" not in table_spec.parameters:
            raise SyncError(SYNC_ERROR_CODE.PARAMETER_NOT_FOUND, "No path parameter found when initialising TsvWriter.")
        self.path = join(dirname(realpath(__file__)), table_spec.parameters["file_path"])

    def get_columns(self):
        return [ DataColumn(COLUMN_TYPE.TEXT, my_f) for my_f in self._read_header() ]

    def _read_header(self):
        try:
            with open(self.path, 'r', encoding="utf") as f:
                reader = csv.DictReader(f, delimiter="\t")
                return reader.fieldnames
        except:
            raise SyncError(SYNC_ERROR_CODE.FILE_ERROR)

    def _read_records(self, limit : int = -1, next_iterator = None, mapping : DataMap = None) -> DataSet:
        if mapping is None: # assume all columns are strings
                cols = [ DataColumn(COLUMN_TYPE.TEXT, my_f) for my_f in self._read_header() ]
                ds_format = DataSetFormat()
        else: 
            cols = [ DataColumn(COLUMN_TYPE.TEXT, my_f) for my_f in mapping.columns ]
            ds_format = mapping.format

        text_dataset = DataSet(cols, format=ds_format)
        
        try:
            with open(self.path, 'r', encoding="utf-8") as f:
                # this is in line with approach to transforming datasets -> i.e. include only columns that are part of mapping
                reader = csv.DictReader(f, delimiter="\t", fieldnames=text_dataset.column_names)

                next(reader) # skip header row

                for record in reader:
                    text_dataset.add_record(record)
        except:
            raise SyncError(SYNC_ERROR_CODE.FILE_ERROR)
        
        if mapping is not None: return text_dataset.remap(mapping).op_returns["remapped_data"]
        else: return text_dataset

    async def read_records(self, limit : int = -1, next_iterator = None, mapping : DataMap = None):
        return self._read_records(limit, next_iterator, mapping)

    def read_records_sync(self, limit : int = -1, next_iterator = None, mapping : DataMap = None):
        return self._read_records(limit, next_iterator, mapping)