from enum import Enum

class MERGE_TYPE(Enum):
    APPEND = 0
    SOFT_MERGE = 1
    HARD_MERGE = 2

class NOTION_COLUMN_TYPE(Enum):
    TEXT = 0
    SELECT = 1
    MULTI_SELECT = 2 # typically used for tags

class ANKI_FIELD_TYPE(Enum):
    TEXT = 0
    TAGS = 1 # technically this is just text as well, but it needs some special text processing

class SyncManager(object):

    # Must have options to perform a primary-key merge (overwrite / fill blanks) in addition to simple append
    # Modes
    # -- Append - just add the records from the other source to the current source.
    # -- Soft Merge (must specify merge column) - merge records by merge column, filling any blanks with data from source.
    # -- Hard Merge (must specify merge column)
    #

    def download(key, database, target_card_type, target_deck, mapping, merge_type=MERGE_TYPE.SOFT_MERGE):
        # get database columns
        # get target card type
        # verify mappings between target_card_type and target_deck
        # if mappings are good, then:
        # -- for each record:
        # ---- create a new anki card
        # ---- fill fields using mapping and source card
        pass

    def upload(key, database, source_card_type, source_deck, mapping):
        # get database columns
        # get target card type
        # verify mappings between target_card_type and target_deck
        # if mappings are good, then:
        # -- for each card:
        # ---- create a new record
        # ---- fill fields using mapping and source card
        pass

    def get_anki_fields(card_type):

        pass

    def list_databases(key):
        # notion search api with filter
        pass

    def list_database_columns(key, database):
        pass

sync = SyncManager()