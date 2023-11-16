from abc import ABC, abstractmethod
import json

class PlatformAdapter(ABC):
    pid = None
    config = None

    def load_config(self, path="config.json"):
        """
        Load configuration from a JSON file into self.config.

        :param path: The path to the configuration file.
        """ 
        with open(path, "r") as f:
            self.config = json.load(f)

    @abstractmethod
    def start_worker(self, options={}):
        """
        Start the FaaS worker and store the process ID (pid) in self.pid.

        :param options: Optional arguments, if needed.
        :return: 0 if successful, -1 otherwise.
        """ 
        pass

    @abstractmethod
    def kill_worker(self, options={}):
        """
        Kill the FaaS worker. Use self.pid to force kill the worker if needed.

        :param options: Optional arguments, if needed.
        :return: 0 if successful, -1 otherwise.
        """
        pass

    @abstractmethod
    def deploy_func(self, func_config):
        """
        Deploy the given function.

        :param func_config: Dictionary containing function information.
            - func_config["name"]: name of the function.
            - func_config["code"]: code of the function as a string.
            - func_config["requirements_in"]: requirements.in file as a string.
            - func_config["requirements_txt"]: requirements.txt file as a string.
        """
        pass

    @abstractmethod
    def invoke_func(self, func_name, options={}):
        """
        Invoke the deployed function.

        :param func_name: Name of the function to invoke. 
        :param options: Optional arguments, if needed.
        :return: Returned output of the function invocation.
        """
        pass