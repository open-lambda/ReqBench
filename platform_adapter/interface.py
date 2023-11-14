from abc import ABC, abstractmethod

class PlatformAdapter(ABC):
    pid = None
    @abstractmethod
    def start_worker(self, options={}):
        pass

    @abstractmethod
    def kill_worker(self, options={}):
        pass

    @abstractmethod
    def deploy_func(self):
        pass

    @abstractmethod
    def invoke_func(self, url, req_body=None):
        pass