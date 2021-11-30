  Import Structure

- anki_sync.py
    - sync.py
        - dataset.py

# TO-DO

## Critical ##

## High Priority ##

* Update dataset to include RowIDs for sorting / consistency in iterations. Update write_records methods to iterate with RowIds, not clone of dataset (memory efficiency!)

## Mid Priority ##

* Implement optional indexing in dataset class.

## Low Priority ##

## Optional ##

## Unsorted ##
  * improve merge algorithm & test - to make sure right dataset records are also included (through lower priority)
  * finish writing merge tests
    * write record_checker function to check if all records in a dataset match (principally for automated testing)
  * Write method to copy DataRecord, removing unused fields (also preventing pointers from being created)
  * Improve tests for merge_column, append_column to make them not rely on manual inspection
  * Write methods to write / read from CSV & JSON formats (probably sync layer responsibility)
  * Outstanding tests:
    * remap
        * checking the output of remap
    * DataRecord.__eq__
        * type guard
        * column mismatch
        * data mismatch
        * success
    * DataSet.drop_column
        * column name guard
        * success
    * DateSet.merge
        * guard to ensure datasets have same spec
        * 
    * others...
Write method to replace Nones with some other value (probably a default) for the sake of interfacing.

Note: method to generate mapping from another data set is on the GUI model layer (it needs user input to map both sources), not dataset layer.