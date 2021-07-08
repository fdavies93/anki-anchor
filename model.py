from .config import ConfigManager

class ModelManager():
    def load_config(self):
        self.config = ConfigManager()

    def get_config(self):
        return self.config

model = ModelManager()