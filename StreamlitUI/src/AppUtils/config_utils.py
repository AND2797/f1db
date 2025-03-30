import configparser
import pathlib

class Config:
    _instance = None

    def __new__(cls, config_path):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config(config_path)
        return cls._instance

    def _load_config(self, config_path):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    def get_property(self, section, key, fallback=None):
        return self.config.get(section, key, fallback=fallback)


def get_property(section, key, fallback=None):
    env_name = "dev"
    # fix env
    # better way to get project root? this one assumes where config_utils.py is
    project_root = pathlib.Path(__file__).parent.parent.resolve()
    conf_file = project_root.joinpath("config_files", env_name, "config_files.txt")
    conf = Config(str(conf_file))
    return conf.get_property(section, key, fallback)
