from abc import ABC, abstractmethod
import json

class PlatformAdapter(ABC):
    pid = None
    config = None

    def load_config(self, path="config.json"):
        with open(path, "r") as f:
            self.config = json.load(f)

    @abstractmethod
    def start_worker(self, options={}):
        pass

    @abstractmethod
    def kill_worker(self, options={}):
        pass

    @abstractmethod
    def deploy_func(self, func_config):
        pass

    @abstractmethod
    def invoke_func(self, func_name, options={}):
        pass