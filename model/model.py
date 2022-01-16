from .config import ConfigManager
from core.sync import *

class ModelManager():
    def __init__(self):
        self.load_config()
        self.sync = sync

    def load_config(self):
        self.config = ConfigManager()

    def get_config(self):
        return self.config

    def get_notion_key(self):
        return self.config["notion_key"]

    def get_merge_mode(self):
        return self.config["merge_mode"]

    def save_notion_key(self, new_key):
        self.config["notion_key"] = str(new_key)
        self.config.save()

    def save_merge_mode(self, new_mode):
        self.config["merge_mode"] = new_mode
        self.config.save()

model = ModelManager()