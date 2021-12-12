from enum import Enum, IntEnum
from dataclasses import dataclass, field
from os import error
from typing import Collection, Type, Union, List, Dict, Set
from datetime import datetime
import copy

class MERGE_TYPE(IntEnum):
    APPEND = 0
    APPEND_NO_DUPLICATES = 1
    SOFT_MERGE = 2
    HARD_MERGE = 3

class COLUMN_TYPE(IntEnum):
    TEXT = 0 # Most Anki fields; Notion tag field
    SELECT = 1 # Notion select field
    MULTI_SELECT = 2 # Notion multi-select / Anki tags field
    DATE = 3 # Notion date field

class COLUMN_ERROR_CODE(IntEnum):
    COLUMN_NOT_FOUND = 0,
    COLUMN_ALREADY_EXISTS = 1,
    COLUMN_TYPE_INCOMPATIBLE = 2

class DATA_ERROR_CODE(IntEnum):
    DATA_CANNOT_CONVERT = 0,
    DATA_TYPE_INCOMPATIBLE = 1,
    DATA_COLUMNS_INCOMPATIBLE = 2

class OP_STATUS_CODE(IntEnum):
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
class DataSetFormat:
    multiselect_delimiter: str = ","
    time_format: str = "%b %d, %Y %I:%M %p"

@dataclass
class DataMap:
    columns: dict
    format: DataSetFormat

@dataclass
class OperationStatus:
    operation: str
    status: OP_STATUS_CODE
    op_returns: dict
    non_critical_errors: int = 0

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

    def __init__(self,columns:list,records:list=[],format:DataSetFormat = DataSetFormat()):
        # columns is always a list of DataColumns
        # records can be a list of dicts
        # default time format is identical to Notion's
        if not isinstance(format, DataSetFormat):
            raise TypeError(format)
        self._columns = []
        self._deleted_column_ids = []
        self.records : list[DataRecord] = []
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
        ''' Adds record to dataset. 
        In case where record has fewer columns than the dataset it's inserting into, extra columns are filled with None.
        In case where record has columns that don't exist in the dataset, these columns are ignored.'''
        if isinstance(record, dict): 
            r = self._add_record_from_dict(record)
        else:
            d = self.record_to_dict(record)
            r = self._add_record_from_dict(d)
        return r

    def remap(self, map : DataMap) -> OperationStatus:
        ''' Changes column names and types according to the map object, until they all match the desired mapping.
        Returns a copy of self with columns remapped.
        map: A dictionary, with the source_column as key and DataColumn (i.e. target name and type) as value. Also 
        '''
        clone = copy.deepcopy(self)
        type_change_results = {}

        missing_map_source = []
        for map_entry in map.columns:
            if map_entry not in clone.column_names:
                missing_map_source.append(map_entry)
        # needs to happen before clone is modified

        for col in self.columns:
            if col.name not in map.columns:
                # it doesn't go anywhere, so delete it
                # unclear what best behaiour is in situation where there's a desired source column that's not in the dataset
                # most likely this is a problem with /generating/ the mapping or the mapping being outdated, not the remapping process, 
                # so for now we're simply returning some information on this in the OperationStatus
                clone.drop_column(col.name)
            else:
                if col.type != map.columns[col.name].type:
                    type_change_results[col.name] = clone.change_column_type(col.name, map.columns[col.name].type)
                if map.columns[col.name].name != col.name:
                    # print( self.column_names )
                    # print( "Renaming " + col.name + " to " + map.columns[col.name].name )
                    clone.rename_column(col.name,map.columns[col.name].name) # this could be dangerous - we're modifying what we're looping over

        total_errors = sum([x.non_critical_errors for x in type_change_results.values()])
        result_info = {
            "remapped_data": clone,
            "type_change_results": type_change_results,
            "missing_sources": missing_map_source
        }
        return OperationStatus("remap", OP_STATUS_CODE.OP_SUCCESS, non_critical_errors=total_errors,op_returns=result_info)

    @property
    def column_names(self) -> list:
        '''Returns current list of column names. Note that this is not the map of column names to indexes.'''
        return [k for k in self._column_names]

    @property
    def columns(self) -> List[DataColumn]:
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

    def get_column(self, key : str) -> DataColumn:
        return self._columns[self.get_column_index(key)]

    def rename_column(self, old_name, new_name):
        # print (self._column_names)
        # print (old_name)
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

        # simply delisting column is a lot more efficient
        # for record in self.records:
        #     record._column_names = self._column_names
        #     del record._fields[index]

    def change_column_type(self, source_column : str, new_type : COLUMN_TYPE, new_column_name : str = None, inplace = True):
        ''' Changes column type, modifying in-place by default or creating a new column if given a name. '''
        if source_column not in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_NOT_FOUND, source_column)

        source_type = self.get_column(source_column).type

        if source_type not in self.CONVERT_DICT:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_TYPE_INCOMPATIBLE)
        if new_type not in self.CONVERT_DICT[source_type]:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_TYPE_INCOMPATIBLE)
        if new_column_name in self._column_names:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_ALREADY_EXISTS)
        # Create new column or set to modify in-place.
        if not inplace:
            if new_column_name != None: new_nm = new_column_name
            else: new_nm = source_column
            while new_nm in self._column_names:
                new_nm += "_m"
            new_col = DataColumn(new_type, new_nm)
            self.add_column(new_col)
            dest_column = new_nm
        else:
            if new_column_name != None:
                self.rename_column(source_column, new_column_name)
                dest_column = new_column_name
            else: dest_column = source_column
            self._columns[self._column_names[dest_column]].type = new_type
            # changing column type flag - but we haven't done anything to the data yet
                

        # if new_column_name != None:
        #     new_col = DataColumn(new_type, new_column_name)
        #     self.add_column(new_col)
        #     dest_column = new_column_name
        # else:
        #     dest_column = source_column

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
        
        return OperationStatus("change_column_type", OP_STATUS_CODE.OP_SUCCESS, non_critical_errors=cannot_convert, op_returns={"new_column_name": dest_column})
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

    def make_write_safe(self, types : Dict[COLUMN_TYPE,COLUMN_TYPE]) -> OperationStatus:
        ''' returns copy of dataset with unsafe columns converted to native values (e.g. datetime to string) 
        and a reference for which columns were thus converted '''

        clone = copy.deepcopy(self)

        unsafe_columns = []
        for col in clone.columns:
            if col.type in types:
                unsafe_columns.append(col.name)
        for unsafe_col in unsafe_columns:
            col_type = clone.get_column(unsafe_col).type
            clone.change_column_type(unsafe_col, types[col_type])

        status = OperationStatus("make_write_safe", OP_STATUS_CODE.OP_SUCCESS, { "converted_columns": unsafe_columns, "safe_data": clone })

        return status

    def get_uniques(self) -> Dict[str, Set[str]]:
        ''' Current implementation is extremely inefficient as it calculates the values on-the-fly. 
            This is a major motivation for adding caching features to the DataSet class. '''
        return self.calculate_uniques()

    def calculate_uniques(self) -> Dict[str, Set[str]]:
        ''' This is not an efficient method, but there is no more efficient way to do this. Caching will help. '''
        select_columns : dict[str, Set[str]] = {}
        for column in self.columns:
            select_columns[column.name] = set()
        for record in self.records:
            for col in select_columns: # i.e. keys
                vals = select_all(record[col])
                for val in vals:
                    select_columns[col].add(val)
        return select_columns        

    def equivalent_to(self, other : 'DataSet', key_column : str = None):
        ''' Checks if dataset is equivalent to another dataset: i.e. its columns are the same and its records can be matched to exactly one other record in the other dataset. '''
        # TODO: Add function to determine the optimal index for a key, to optimise when key isn't provided
        if not have_same_columns(self, other): 
            # print ("Columns differ.")
            # print (self.columns)
            # print (other.columns)
            return False
        if key_column != None and (key_column not in self.column_names or key_column not in other.column_names):
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_NOT_FOUND)
        if len(self.records) == 0 and len(other.records) == 0: return True # they're both an empty set with the same column spec

        self_indexes = build_all_key_indexes(self)
        other_indexes = build_all_key_indexes(other)

        if key_column == None:
            # determine optimal key column (one with most bins)
            max_bins = 0
            kc = None
            for col in self_indexes:
                if len(self_indexes[col]) != len(other_indexes[col]):
                    # indexes differ, therefore data is different
                    # print ("Different indexes.")
                    return False
                else:
                    # same number of bins but might not be the same data
                    cur_bins = len(self_indexes[col])
                    if cur_bins > max_bins: 
                        max_bins = cur_bins
                        kc = col

        else: kc = key_column

        return self._check_equivalent_to(self_indexes[kc],other_indexes[kc])

    def _check_equivalent_to(self, self_index : dict, other_index : dict):
        ''' Using a column index generated by build_key_index or build_all_key_indexes to check equivalence of two datasets. '''
        # check equivalence
        # TODO: 
        # appears to only be checking value of first record...
        for self_key in self_index:
            # a key doesn't exist in the other index - they're not the same data
            if self_key not in other_index: 
                # print ("Key not in other index.")
                return False
            # need to use indexes in the other_r section
            other_records = select_all(other_index[self_key])
            other_to_check = [x for x in range(len(other_records))]
            self_records = select_all(self_index[self_key])
            self_to_check = [x for x in range(len(self_records))]
            if len(other_to_check) != len(self_to_check): return False
            # setting up a *list of indexes* to avoid manipulating the record objects inplace
            self_i_i = 0
            while self_i_i < len(self_to_check):
                other_i_i = 0
                self_i = self_to_check[self_i_i]
                other_r = None
                self_r = self_records[self_i]
                while other_i_i < len(other_to_check):
                    other_i = other_to_check[other_i_i]
                    other_r = other_records[other_i]
                    if other_r == self_r:
                        other_to_check.remove(other_i)
                        self_to_check.remove(self_i)
                        self_i_i = -1 # go to start of new list
                        break
                    other_i_i += 1
                if (len(self_to_check) == 0): break
                self_i_i += 1
            if len(self_to_check) > 0 or len(other_to_check) > 0: 
                # print ("Index length is unequal.")
                return False 
            # i.e. there's not exactly 1 match for every record
        return True

    def find_optimal_index(self):
        for column in self.column_names:
            pass

    def _text_to_select(self, txt):
        return txt 
        # difference between select and text is largely determined by external (i.e. data source) representations, not the data itself

    def _text_to_multiselect(self, txt:str):
        return txt.split(self.format.multiselect_delimiter)

    def _text_to_date(self, txt):
        return datetime.strptime(txt,self.format.time_format)

    def _date_to_text(self, date):
        return datetime.strftime(date, self.format.time_format)

    def _multiselect_to_text(self, multi_select: list):
        return self.format.multiselect_delimiter.join(multi_select)

    def _select_to_text(self, select):
        return select
        # difference between select and text is largely determined by external (i.e. data source) representations, not the data itself

def select_first_or_only(input):
    if isinstance(input, list):
        return input[0]
    else: return input

def select_all(input) -> list:
    if isinstance(input,list):
        return input
    else: return [input]

def combine_columns(left : DataSet, right : DataSet, left_join: bool = True, right_join : bool = True, inner_join : bool = True):
    if not left_join and not right_join and not inner_join:
        return DataSet() # no records or columns
    # if not have_same_columns(left, right): raise DataError(DATA_ERROR_CODE.DATA_COLUMNS_INCOMPATIBLE)

    columns_left = { cn: left.get_column(cn) for cn in left.column_names }
    columns_right = { cn: right.get_column(cn) for cn in right.column_names }

    left_col_set = set( cn for cn in left.column_names )
    right_col_set = set( cn for cn in right.column_names )

    inner_cols_set = left_col_set.intersection(right_col_set)
    exclusive_left_col_set = left_col_set.difference(right_col_set)
    exclusive_right_col_set = right_col_set.difference(left_col_set)

    new_columns = []

    # always add columns in both sets
    for col_name in inner_cols_set:
        if columns_left[col_name].type != columns_right[col_name].type:
            raise ColumnError(COLUMN_ERROR_CODE.COLUMN_TYPE_INCOMPATIBLE, "Columns "+col_name+" have same name but differing types; cannot merge data.") 
        else: new_columns.append(columns_left[col_name])

    if left_join:
        for col_name in exclusive_left_col_set: new_columns.append(columns_left[col_name])

    if right_join:
        for col_name in exclusive_right_col_set: new_columns.append(columns_right[col_name])

    return DataSet(new_columns)

def merge(left : DataSet, right : DataSet, left_key : str, right_key : str, overwrite = False, left_join = True, right_join = True, inner_join = True) -> DataSet:
    ''' Defaults to the equivalent of a full outer join (left, right, and inner records are all retained.) '''

    # if not left_join and not right_join and not inner_join:
    #     return DataSet() # no records or columns
    # # if not have_same_columns(left, right): raise DataError(DATA_ERROR_CODE.DATA_COLUMNS_INCOMPATIBLE)

    # columns_left = { cn: left.get_column(cn) for cn in left.column_names }
    # columns_right = { cn: right.get_column(cn) for cn in right.column_names }

    # left_col_set = set( cn for cn in left.column_names )
    # right_col_set = set( cn for cn in right.column_names )

    # inner_cols_set = left_col_set.intersection(right_col_set)
    # exclusive_left_col_set = left_col_set.difference(right_col_set)
    # exclusive_right_col_set = right_col_set.difference(left_col_set)

    # new_columns = []

    # # always add columns in both sets
    # for col_name in inner_cols_set:
    #     if columns_left[col_name].type != columns_right[col_name].type:
    #         raise ColumnError(COLUMN_ERROR_CODE.COLUMN_TYPE_INCOMPATIBLE, "Columns "+col_name+" have same name but differing types; cannot merge data.") 
    #     else: new_columns.append(columns_left[col_name])

    # if left_join:
    #     for col_name in exclusive_left_col_set: new_columns.append(columns_left[col_name])

    # if right_join:
    #     for col_name in exclusive_right_col_set: new_columns.append(columns_right[col_name])

    # new_set = DataSet(new_columns)

    new_set = combine_columns(left, right, left_join, right_join, inner_join)

    index_left = build_key_index(left, left_key)
    index_right = build_key_index(right, right_key)
    left_keys = set(k for k in index_left.keys())
    right_keys = set(k for k in index_right.keys())

    if inner_join:
        for k in left_keys.intersection(right_keys):
            # 
            # force_uniques forces only a single unique outcome to an inner join 
            # i.e. it assumes keys are unique, although this might be untrue
            # removed force_uniques option for making result of operations unpredictable
            # (it makes result dependent on order of storage of records)
            #
            # if force_uniques:
            #     lrs = [select_first_or_only(index_left[k])]
            #     rrs = [select_first_or_only(index_right[k])]
            # else:
            lrs = select_all(index_left[k])
            rrs = select_all(index_right[k])

            for lr in lrs:
                for rr in rrs:        
                    new_record = merge_records(lr, rr, new_set, overwrite=overwrite)
                    new_set.add_record(new_record)

    if left_join:
        for k in left_keys.difference(right_keys):
            for record in select_all(index_left[k]):
                new_set.add_record(record)

    if right_join:
        for k in right_keys.difference(left_keys):
            for record in select_all(index_right[k]):
                new_set.add_record(record)

    return new_set

def merge_records(left: DataRecord, right: DataRecord, dataset:DataSet, overwrite=False):
    new_fields = {}

    for col_name in dataset.column_names:
        i = dataset.get_column_index(col_name)
        if col_name in left._column_names and col_name in right._column_names:
            # in both sources
            right_val = right[col_name]
            left_val = left[col_name]
            if (left_val != None and right_val == None) or overwrite:
                new_fields[i] = left_val
            else: new_fields[i] = right_val # note this includes scenario where both are None
        elif col_name in left._column_names:
            # only in left
            left_val = left[col_name]
            new_fields[i] = left_val
        elif col_name in right._column_names:
            # only in right
            right_val = right[col_name]
            new_fields[i] = right_val
        else:
            # in neither
            new_fields[i] = None

    return DataRecord(dataset._column_names, new_fields)

def have_same_columns(left: DataSet, right: DataSet) -> bool:
    ''' Returns true if columns are identical between two datasets. '''
    left_column_names = set(left.column_names)
    right_column_names = set(right.column_names)
    if len(left_column_names.difference(right_column_names)) > 0: 
        # print("Column names differ.")
        return False # column names aren't the same
    for left_key in left_column_names:
        if left.get_column(left_key) != right.get_column(left_key): 
            # print ("Column types differ in " + left_key)
            return False # column types are different
    return True

def append(left: DataSet, right: DataSet, left_key: str, right_key: str, ignore_duplicates: bool = False):

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

def new_append(left: DataSet, right: DataSet, left_join: bool = True, right_join: bool = True, inner_join : bool = True):
    ds = combine_columns(left, right, left_join, right_join, inner_join)    
    ds.add_records(left.records)
    ds.add_records(right.records)
    return ds
        
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

def build_all_key_indexes(ds:DataSet):
    indexes = {}
    for col in ds.columns:
        if col.type != COLUMN_TYPE.MULTI_SELECT:
            indexes[col.name] = {}
    
    for r in ds.records:
        for col_name in indexes:
            if r[col_name] not in indexes[col_name]: # no protection against using list as an index
                indexes[col_name][r[col_name]] = r
            elif isinstance( indexes[col_name][r[col_name]] , list ):
                indexes[col_name][r[col_name]].append(r)
            else:
                indexes[col_name][r[col_name]] = [indexes[col_name][r[col_name]],r]
    # may raise error when faced with datetimes - let's see
    return indexes