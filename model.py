from .config import ConfigManager
from .sync import *

class ModelManager():
    def __init__(self):
        self.load_config()

    def load_config(self):
        self.config = ConfigManager()

    def get_config(self):
        return self.config

    def get_notion_key(self):
        return self.config["notion_key"]

    def save_notion_key(self, new_key):
        self.config["notion_key"] = str(new_key)
        self.config.save()

model = ModelManager()