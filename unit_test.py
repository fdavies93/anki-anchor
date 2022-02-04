from json.encoder import JSONEncoder
from os import unlink, write

import anki
from core.dataset import *
from core.sync.sync_notion import NotionReader, NotionWriter
from core.sync.sync_types import *
from core.sync.sync_tsv import *
from core.sync.sync_json import *
import unittest
from os.path import dirname, exists, join, realpath
import json
from datetime import date, datetime
import asyncio
import copy
from anki_testing import anki_running
import time
import locale

class AnkiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.abs_path = os.getcwd()
        cols = [
            DataColumn(COLUMN_TYPE.TEXT, "id"),
            DataColumn(COLUMN_TYPE.DATE, "date"),
            DataColumn(COLUMN_TYPE.MULTI_SELECT, "multiselect"),
            DataColumn(COLUMN_TYPE.SELECT, "select"),
            DataColumn(COLUMN_TYPE.TEXT, "bad_data")
        ]
        records = [
            {
                "id": "0",
                "date": datetime(1994, 3, 23, 12, 1),
                "multiselect": ['0','1','2','3','4'],
                "select": "0",
                "bad_data": "xyz",
            },
            {
                "id": "1",
                "date": datetime(1995, 3, 24, 12, 2),
                "multiselect": ['1','2','3','4','5'],
                "select": "1",
                "bad_data": "000 000 000"
            },
            {
                "id": "2",
                "date": datetime(1996, 3, 25, 12, 3),
                "multiselect": ['2','3','4','5','6'],
                "select": "2",
                "bad_data": None
            },
            {
                "id": "3",
                "date": datetime(1997, 3, 26, 12, 4),
                "multiselect": ['3','4','5','6','7'],
                "select": "3",
                "bad_data": None
            }
        ]

        self.ds = DataSet(cols, records)


    def add_test_collection(self):
        aw = self.module.AnkiWriter({})
        aw.create_table(self.ds, "New Card Type")
        ar = self.module.AnkiReader({})
        print(ar.get_tables())
        # aw.add_collection()

    def test_anki_startup(self):
        with anki_running() as anki_app:
            import model.sync_anki as sa
            self.module = sa
            self.app = anki_app

            with self.subTest(): # test creating a collection and adding records
                aw = self.module.AnkiWriter({})
                ar = self.module.AnkiReader({})
                table = aw.create_table(self.ds, "Total Write Test")
                aw.set_table(table)
                aw._write_records(self.ds)
                ar.set_table(table)
                records = ar.read_records_sync().records
                tsv = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/anki_write_all.tsv", "absolute_path": self.abs_path}, "anki_write_all"))
                tsv.create_table_sync(records)

            with self.subTest():
                aw = self.module.AnkiWriter({})
                ar = self.module.AnkiReader({})
                table = aw.create_table(self.ds, "Iterative Write Test")
                aw.set_table(table)
                
                it = aw._write_records(self.ds, 1)
                while not it.done:
                    it = aw._write_records(self.ds, 1, it)
                ar.set_table(table)
                records = ar.read_records_sync().records
                tsv = TsvWriter(TableSpec(DATA_SOURCE.TSV, {"file_path": "./test_output/anki_write_it.tsv", "absolute_path": self.abs_path}, "anki_write_it"))
                tsv.create_table_sync(records)

if __name__ == '__main__':
    unittest.main()