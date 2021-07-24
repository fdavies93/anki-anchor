from enum import Enum
from dataclasses import dataclass, field
from typing import Union

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

class COLUMN_ERROR_CODE(Enum):
    COLUMN_NOT_FOUND = 0

class ColumnError(ValueError):
    def __init__(self, error_code: COLUMN_ERROR_CODE, message="Column error."):
        self.error_code = error_code
        super().__init__(message)

@dataclass
class DataColumn:
    type: COLUMN_TYPE
    name: str

@dataclass
class DataRecord:
    _column_names: dict # ties name to index
    _fields: dict # always a dict where index is column index

    def __getitem__(self,key):
        i = self._column_names[key]
        return self._fields[i]

    def asdict(self):
        record_dict = {}
        for col_name in self._column_names:
            col_i = self._column_names[col_name]
            record_dict[col_name] = self._fields[col_i]
        return record_dict
    
class DataSet:
    columns: list
    records: list
    iterator: str
    _column_names: dict # ties name to index

    def __init__(self,columns:list,records:list=[]):
        # columns is always a list of DataColumns
        # records can be a list of dicts
        self.columns = []
        self.records = []
        self._column_names = {}
        for i in range(0,len(columns)):
            col = columns[i]
            new_col = DataColumn(col.type,col.name)
            self._column_names[col.name] = i
            self.columns.append(new_col)
        
        for r in records:
            self.add_record(r)

    def add_records(self, records:list) -> list:
        return [self.add_record(rec) for rec in records]

    def add_record(self, record:Union[dict,DataRecord]) -> DataRecord:
        if isinstance(record, dict): 
            r = self._add_record_from_dict(record)
        else:
            d = self.record_to_dict(record)
            r = self._add_record_from_dict(d)
        return r


    def record_to_dict(self, record:DataRecord):
        record_dict = {}
        for col_name in self._column_names:
            record_dict[col_name] = record[col_name]
        return record_dict

    def _add_record_from_dict(self, record):
        record_fields = {}
        for k, v in self._column_names.items():
            if k in record: record_fields[v] = record[k]
            else: record_fields[v] = None
            # just ignore any additional fields; fill spaces that don't have a column with None
        r = DataRecord(self._column_names, record_fields)
        self.records.append(r)
        return r

    def rename_column(self, old_name, new_name):
        if old_name not in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_NOT_FOUND)
        self._column_names[new_name] = self._column_names[old_name]
        self.columns[self._column_names[new_name]].name = new_name # rename the actual column object
        del self._column_names[old_name]
        for record in self.records:
            record._column_names = self._column_names
            
    def get_column_index(self, key):
        if key not in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_NOT_FOUND)
        return self._column_names[key]

    def get_column(self, key):
        return self.columns[self.get_column_index(key)]

def select_first_or_only(input):
    if isinstance(input, list):
        return input[0]
    else: return input

def select_all(input):
    if isinstance(input,list):
        return input
    else: return [input]

def merge(left, right, left_key, right_key, overwrite = False):
    index_left = build_key_index(left, left_key)
    index_right = build_key_index(right, right_key)
    new_set = DataSet(left.columns)
    for k in index_left: # left _always_ takes priority
        if k in index_right and right.get_column(right_key) == left.get_column(left_key):
            lr = select_first_or_only(index_left[k])
            rr = select_first_or_only(index_right[k])
            new_record = merge_records(lr, rr, overwrite)
            new_set.add_record(new_record)
    return new_set

def merge_records(left: DataRecord, right: DataRecord, overwrite=False):
    new_fields = {}
    for name, i in left._column_names.items():
        right_val = right[name]
        left_val = left[name]
        if (left_val != None and right_val == None) or overwrite:
            new_fields[i] = left_val
        else: new_fields[i] = right_val    
    return DataRecord(left._column_names, new_fields)


def append(left: DataSet, right: DataSet, left_key: str, right_key: str, ignore_duplicates: bool = True):
    index_left = build_key_index(left, left_key)
    index_right = build_key_index(right, right_key)
    new_set = DataSet(right.columns)
    for k in index_right:
        for r in select_all(index_right[k]):
            new_set.add_record(r)
        # insert all right data records
    for k in index_left:
        if k not in index_right or (k in index_right and not ignore_duplicates):
            if ignore_duplicates: 
                r = select_first_or_only(index_left[k])
                new_set.add_record(r)
            else:
                for r in select_all(index_left[k]):
                    new_set.add_record(r)
            # add left data records where there's no matching key in right or if duplicates are allowed
    return new_set
        
def build_key_index(ds:DataSet, key_col):
    index = {}
    for r in ds.records:
        if r[key_col] not in index:
            index[r[key_col]] = r
        elif isinstance(index[r[key_col]], list):
            index[r[key_col]].append(r)
        else:
            index[r[key_col]] = [index[r[key_col]],r]
    return index