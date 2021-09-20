from json.encoder import JSONEncoder
from dataset import *
import unittest
from os.path import dirname, exists, join, realpath
import json
from datetime import date, datetime

def write_out(input,relative_path):
    saved_path = join(dirname(realpath(__file__)), relative_path)
    with open(saved_path, 'w', encoding='utf-8') as f:
            rs = RecordSerializer(indent=4)
            f.write(rs.encode(input))

class RecordSerializer(JSONEncoder):
    def default(self,o):
        if isinstance(o,DataRecord):
            return o.asdict()
        return json.JSONEncoder.default(self, o)

class TestDataSet(unittest.TestCase):
    def setUp(self) -> None:
        self.cols = [
            DataColumn(COLUMN_TYPE.TEXT, "title"),
            DataColumn(COLUMN_TYPE.TEXT, "description"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "tags")
        ]
        
        self.advanced_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "title"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.DATE, "date")
        ]
        self.records = [
            {
                "title": "record_1",
                "description": "this is the first record",
                "tags": ["0", "3", "5"]
            },
            {
                "title": "record_2",
                "description": "this is the second record",
                "tags": ["1", "2", "3"]
            },
            {
                "title": "too many fields",
                "description": "should cut down",
                "tags": ["5", "6", "7"],
                "extra1": "what is this",
                "extra2": "idk"
            },
            {
                "title": "too many fields",
                "description": "should cut down",
                "tags": ["5", "6", "7"],
            },
            {
                "title": "too few fields",
                "description": "should fill with Nones"
            },
            {
                "title": "too few fields",
                "description": "should fill with Nones",
                "tags": None
            },
            {
                "title": "merge_1",
                "description": None,
                "tags": None
            },
            {
                "title": "merge_2",
                "description": "This is merge_2.",
                "tags": ["test", "replace"]
            },
            {
                "title": None,
                "description": "Title should be merge_3.",
                "tags": None
            }
        ]
        self.merge_left_records = [
            {
                "title": "merge_1",
                "description": "This is merge_1 from left set.",
                "tags": ["0", "1"]
            },
            {
                "title": "merge_2",
                "description": "This is merge_2 from left set.",
                "tags": ["0", "2"]
            }
        ]

    def test_get_column(self):
        ds = DataSet(self.cols)
        self.assertEqual( ds.get_column("title"), self.cols[0] )
        self.assertEqual( ds.get_column("description"), self.cols[1] )
        self.assertEqual( ds.get_column("tags"), self.cols[2] )
        self.assertRaises(ColumnError, ds.get_column, "snoog")

    def test_add_record(self):
        ds = DataSet(self.cols)
        ds.add_record( self.records[0] )
        ds2 = DataSet(self.cols)
        ds2.add_record( self.records[1] )
        ds.add_record( ds2.records[0] )
        self.assertNotEqual(ds2.records[0], ds.records[0])
        self.assertEqual(ds2.records[0], ds.records[1])
        ds.add_records( self.records[2:4] )
        self.assertEqual(ds.records[2], ds.records[3])
        ds.add_records( self.records[4:6] )
        self.assertEqual(ds.records[4], ds.records[5])

    def test_rename_column(self):
        ds = DataSet(self.cols)
        ds.add_records(self.records)
        self.assertEqual(ds.columns[0],DataColumn(COLUMN_TYPE.TEXT,"title"))
        ds.rename_column("title","name")
        self.assertEqual(ds.columns[0],DataColumn(COLUMN_TYPE.TEXT,"name"))
        cols2 = [
            DataColumn(COLUMN_TYPE.TEXT, "name"),
            DataColumn(COLUMN_TYPE.TEXT, "description"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "tags")
        ]
        ds2 = DataSet(cols2)
        renamed = {
                "name": "record_1",
                "description": "this is the first record",
                "tags": ["0", "3", "5"]
        }
        new_rec = ds2.add_record(renamed)
        self.assertEqual(new_rec, ds.records[0])

    def test_record_get_item(self):
        ds = DataSet(self.cols)
        ds.add_record(self.records[0])

    def test_append_records(self):
        ds = DataSet(self.cols)
        ds2 = DataSet(self.cols)
        ds3 = DataSet(self.cols) # benchmark for blunt append
        ds.add_records(self.records)
        ds2.add_records(self.records)
        ds3.add_records(self.records)
        ds3.add_records(self.records)

        append_no_ignore = append(ds, ds2, "title", "title", False)
        append_ignore = append(ds, ds2, "title", "title", True)

        self.assertEqual(append_no_ignore.records, ds3.records)
        write_out(append_no_ignore.records,"./test_output/append_no_ignore.json")
        self.assertEqual(append_ignore.records, ds.records)
        write_out(append_ignore.records,"./test_output/append_ignore.json")
        # the success of these tests might be down to implementation of dict
        # however it's probably fine, and avoids need to write a sort method

    def test_merge_records(self):
        ds = DataSet(self.cols)
        ds2 = DataSet(self.cols)
        ds.add_records(self.records[6:9])
        ds2.add_records(self.merge_left_records)

        merge_soft = merge(ds2, ds, "title", "title")
        merge_hard = merge(ds2, ds, "title", "title", True)

        write_out(merge_soft.records,"./test_output/merge_soft.json")
        write_out(merge_hard.records,"./test_output/merge_hard.json")

    def test_convert_types_correct(self):
        test_format = {"multiselect_delimiter": ",", "time_format": "%b %d, %Y %I:%M %p"}
        ds = DataSet(self.cols, format=test_format)

        # Select
        test_select_str = "select test"
        cur_out = ds.change_data_type("select test", COLUMN_TYPE.TEXT, COLUMN_TYPE.SELECT)
        self.assertEqual(test_select_str, cur_out) # no change as select validity is determined outside the scope of a dataset

        # Multiselect
        test_multiselect_str = "obj1,obj2,obj3,obj4,obj5"
        test_multiselect_list = ["obj1","obj2","obj3","obj4","obj5"]

        cur_out = ds.change_data_type(test_multiselect_str, COLUMN_TYPE.TEXT, COLUMN_TYPE.MULTI_SELECT)
        self.assertListEqual(test_multiselect_list, cur_out)

        cur_out = ds.change_data_type(test_multiselect_list, COLUMN_TYPE.MULTI_SELECT, COLUMN_TYPE.TEXT)
        self.assertEqual(test_multiselect_str, cur_out)

        # Dates

        test_date_str = "Mar 23, 1994 12:01 PM"
        test_date = datetime.strptime(test_date_str, test_format["time_format"])

        cur_out = ds.change_data_type("Mar 23, 1994 12:01 PM", COLUMN_TYPE.TEXT, COLUMN_TYPE.DATE)
        self.assertEqual(test_date, cur_out)

        cur_out = ds.change_data_type(test_date, COLUMN_TYPE.DATE, COLUMN_TYPE.TEXT)
        self.assertEqual(cur_out, test_date_str)

    def test_add_column(self):

        extra_cols = [
            DataColumn(COLUMN_TYPE.TEXT, "title"),
            DataColumn(COLUMN_TYPE.TEXT, "description"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "tags"),
            DataColumn(COLUMN_TYPE.TEXT, "test_column")
        ]

        none_test = [
            {
                "title": "record_1",
                "description": "this is the first record",
                "tags": ["0", "3", "5"],
                "test_column": None
            },
            {
                "title": "record_2",
                "description": "this is the second record",
                "tags": ["1", "2", "3"],
                "test_column": None
            }]

        prefill_test = [
            {
                "title": "record_1",
                "description": "this is the first record",
                "tags": ["0", "3", "5"],
                "test_column": [0,1,2,3]
            },
            {
                "title": "record_2",
                "description": "this is the second record",
                "tags": ["1", "2", "3"],
                "test_column": [0,1,2,3]
            }]

        ds = DataSet(self.cols)
        ds.add_records(self.records[0:2])

        test_col = DataColumn(COLUMN_TYPE.TEXT,"test_column")
        ds.add_column(test_col)

        ds2 = DataSet(extra_cols, records=none_test)
        self.assertEqual(ds.records[0], ds2.records[0])
        self.assertEqual(ds.records[1], ds2.records[1])

        ###

        ds = DataSet(self.cols)
        ds.add_records(self.records[0:2])

        test_col = DataColumn(COLUMN_TYPE.MULTI_SELECT,"test_column")
        ds.add_column(test_col, [0,1,2,3])

        ds2 = DataSet(extra_cols, records=prefill_test)
        self.assertEqual(ds.records[0], ds2.records[0])
        self.assertEqual(ds.records[1], ds2.records[1])


        print(ds.records[1])

if __name__ == '__main__':
    unittest.main()