from enum import Enum
from dataclasses import dataclass, field
from typing import Union
from datetime import datetime
import copy

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
    COLUMN_NOT_FOUND = 0,
    COLUMN_ALREADY_EXISTS = 1,
    COLUMN_TYPE_INCOMPATIBLE = 2

class DATA_ERROR_CODE(Enum):
    DATA_CANNOT_CONVERT = 0,
    DATA_TYPE_INCOMPATIBLE = 1

class OP_STATUS_CODE(Enum):
    OP_SUCCESS = 0,
    OP_FAILURE = 1

class DataError(ValueError):
    def __init__(self, error_code: DATA_ERROR_CODE, message="Data operation error."):
        self.error_code = error_code
        super().__init__(message)

class ColumnError(ValueError):
    def __init__(self, error_code: COLUMN_ERROR_CODE, message="Column error."):
        self.error_code = error_code
        super().__init__(message)

@dataclass
class OperationStatus:
    operation: str
    status: OP_STATUS_CODE
    non_critical_errors: int

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

    def __setitem__(self, key, value):
        i = self._column_names[key]
        self._fields[i] = value

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, DataRecord):
            raise TypeError(o)

        all_columns = set(self._column_names.keys())
        all_columns.update(o._column_names.keys())

        for k in all_columns:
            if k not in self._column_names or k not in o._column_names: # check same number of columns
                return False
            if self[k] != o[k]: # :D - leveraging __getitem__ method to check equality
                return False
        return True

    def asdict(self):
        record_dict = {}
        for col_name in self._column_names:
            col_i = self._column_names[col_name]
            record_dict[col_name] = self._fields[col_i]
        return record_dict

class DataSet:

    def __init__(self,columns:list,records:list=[],format={"multiselect_delimiter": ",", "time_format": "%b %d, %Y %I:%M %p"}):
        # columns is always a list of DataColumns
        # records can be a list of dicts
        # default time format is identical to Notion's
        self._columns = []
        self._deleted_column_ids = []
        self.records = []
        self._column_names = {}
        self.format = format
        for i in range(0,len(columns)):
            col = columns[i]
            new_col = DataColumn(col.type,col.name)
            self._column_names[col.name] = i
            self._columns.append(new_col)

        for r in records:
            self.add_record(r)

        self.CONVERT_DICT = {
            COLUMN_TYPE.TEXT: {
                COLUMN_TYPE.DATE: self._text_to_date,
                COLUMN_TYPE.SELECT: self._text_to_select,
                COLUMN_TYPE.MULTI_SELECT: self._text_to_multiselect,
            },
            COLUMN_TYPE.DATE: {
                COLUMN_TYPE.TEXT: self._date_to_text
            },
            COLUMN_TYPE.SELECT: {
                COLUMN_TYPE.TEXT: self._select_to_text
            },
            COLUMN_TYPE.MULTI_SELECT: {
                COLUMN_TYPE.TEXT: self._multiselect_to_text
            }
        }

    def add_records(self, records:list) -> list:
        return [self.add_record(rec) for rec in records]

    def add_record(self, record:Union[dict,DataRecord]) -> DataRecord:
        if isinstance(record, dict): 
            r = self._add_record_from_dict(record)
        else:
            d = self.record_to_dict(record)
            r = self._add_record_from_dict(d)
        return r

    def remap(self, map : dict):
        ''' Changes column names and types according to the map object, until they all match the desired mapping.
        Returns a copy of the 
        map: A dictionary, with the source_column as key and DataColumn as value.
        '''
        clone = copy.deepcopy(self)
        for col in clone._columns:
            if col.name not in map:
                # it doesn't go anywhere, so delete it
                pass

    @property
    def column_names(self) -> list:
        '''Returns current list of column names. Note that this is not the map of column names to indexes.'''
        return [k for k in self._column_names]

    @property
    def columns(self) -> list:
        '''Returns list of current column definitions - i.e. not the underlying list _columns.'''
        return [self._columns[v] for v in self._column_names.values()]

    def column_to_list(self, column_name) -> list:
        if column_name not in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_NOT_FOUND)
        return [ record[column_name] for record in self.records ]

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

    def get_column_index(self, key) -> int:
        if key not in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_NOT_FOUND)
        return self._column_names[key]

    def get_column(self, key) -> DataColumn:
        return self._columns[self.get_column_index(key)]

    def rename_column(self, old_name, new_name):
        if old_name not in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_NOT_FOUND)
        if new_name in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_ALREADY_EXISTS)
        self._column_names[new_name] = self._column_names[old_name]
        self._columns[self._column_names[new_name]].name = new_name # rename the actual column object
        del self._column_names[old_name]
        for record in self.records:
            record._column_names = self._column_names

    def get_next_column_id(self) -> int:
        ''' Gets next available column id. '''
        if len(self._deleted_column_ids) > 0:
            return self._deleted_column_ids[-1]
        else: 
            return len(self._columns)

    def add_column(self, column: DataColumn, default_val = None):
        if column.name in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_ALREADY_EXISTS)
        
        new_column_id = self.get_next_column_id()

        # recycling old column ids to avoid throwing off previous fields in records
        if len(self._columns) == new_column_id:
            self._columns.append(column)
        else:
            self._columns[new_column_id] = column
            self._deleted_column_ids.pop()

        self._column_names[column.name] = new_column_id

        for record in self.records:
            record._column_names = self._column_names
            record._fields[new_column_id] = default_val

    def drop_column(self, column_name: str):
        if column_name not in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_NOT_FOUND)
        
        index = self._column_names[column_name]
        # del self.columns[index]
        # unfortunately self.columns is actually a list, although not used this way (thankfully) in records
        # current solution, rather than deleting column and requiring update for each record - just delist it from column names
        del self._column_names[column_name]
        self._deleted_column_ids.append(index)

        for record in self.records:
            record._column_names = self._column_names
            del record._fields[index]

    def change_column_type(self, source_column : str, new_type : COLUMN_TYPE, new_column_name : str = None):
        ''' Changes column type, modifying in-place by default or creating a new column if given a name. '''
        if source_column not in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_NOT_FOUND)

        source_type = self.get_column(source_column).type

        if source_type not in self.CONVERT_DICT:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_TYPE_INCOMPATIBLE)
        if new_type not in self.CONVERT_DICT[source_type]:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_TYPE_INCOMPATIBLE)
        if new_column_name in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_ALREADY_EXISTS)
        # Create new column or set to modify in-place.
        if new_column_name != None:
            new_col = DataColumn(new_type, new_column_name)
            self.add_column(new_col)
            dest_column = new_column_name
        else:
            dest_column = source_column

        cannot_convert = 0

        for r in self.records:
            try:
                r[dest_column] = self.change_data_type(r[source_column], source_type, new_type)
            except DataError as err:
                if err.error_code == DATA_ERROR_CODE.DATA_CANNOT_CONVERT:
                    # this is fine and probably a result of user error or incomplete data
                    cannot_convert += 1
                    r[dest_column] = None
                elif err.error_code == DATA_ERROR_CODE.DATA_TYPE_INCOMPATIBLE:
                    # this is not fine and shows that our data structures are wack
                    # it should never actually happen, but could happen if more sophisticated data checking implemented later?
                    raise ColumnError(COLUMN_ERROR_CODE.COLUMN_TYPE_INCOMPATIBLE)
        
        return OperationStatus("change_column_type", OP_STATUS_CODE.OP_SUCCESS, cannot_convert)
        # lets us know about failed conversions so we can alert user to possible data loss

    # here we're changing data type in the INTERNAL representation
    def change_data_type(self, input: object, input_type : COLUMN_TYPE, output_type: COLUMN_TYPE):
        if input_type not in self.CONVERT_DICT:
            raise DataError(DATA_ERROR_CODE.DATA_TYPE_INCOMPATIBLE)
        if output_type not in self.CONVERT_DICT[input_type]:
            raise DataError(DATA_ERROR_CODE.DATA_TYPE_INCOMPATIBLE)
        try:
            output = self.CONVERT_DICT[input_type][output_type](input)
        except:
            raise DataError(DATA_ERROR_CODE.DATA_CANNOT_CONVERT) # this catches invalid conversions without smashing the program
        return output

    def _text_to_select(self, txt):
        return txt 
        # difference between select and text is largely determined by external (i.e. data source) representations, not the data itself

    def _text_to_multiselect(self, txt:str):
        return txt.split(self.format["multiselect_delimiter"])

    def _text_to_date(self, txt):
        return datetime.strptime(txt,self.format["time_format"])

    def _date_to_text(self, date):
        return datetime.strftime(date, self.format["time_format"])

    def _multiselect_to_text(self, multi_select: list):
        return self.format["multiselect_delimiter"].join(multi_select)

    def _select_to_text(self, select):
        return select
        # difference between select and text is largely determined by external (i.e. data source) representations, not the data itself

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
        else: new_fields[i] = right_val # note this includes scenario where both are None
    return DataRecord(left._column_names, new_fields)


def append(left: DataSet, right: DataSet, left_key: str, right_key: str, ignore_duplicates: bool = True):
    index_left = build_key_index(left, left_key)
    index_right = build_key_index(right, right_key)
    new_set = DataSet(right.columns) # make a new, empty dataset with only the valid columns from 1
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