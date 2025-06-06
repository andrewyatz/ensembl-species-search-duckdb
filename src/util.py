import toml
import os


def get_config(path: str = None):
    """Create a config dictionary from the given path, take the location from env variable SEARCH_CONFIG or defaults to config.toml"""
    if path:
        config_path = path
    else:
        config_path = os.environ.get("SEARCH_CONFIG", "config.toml")
    return toml.load(config_path)
