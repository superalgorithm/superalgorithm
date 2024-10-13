import os
from pathlib import Path
import warnings
import yaml


def read_config(yaml_file=None):
    """
    Reads configuration variables from a YAML file (default config.yaml) and any present environment variables
    """
    config = {}
    try:
        yaml_file = find_config() if yaml_file is None else yaml_file
        # Try to read from YAML file if provided
        if yaml_file and os.path.exists(yaml_file):
            with open(yaml_file, "r") as file:
                try:
                    config = yaml.safe_load(file)
                except yaml.YAMLError as e:
                    raise e
    except Exception as e:
        warnings.warn(
            f"Could not find config.yaml or the file is damaged, proceeding with environment variables only. Error: {e}"
        )

    # Read from environment variables
    for key, value in os.environ.items():
        config[key] = value

    return config


def find_config():
    def traverse(start_path):
        current_path = Path(start_path).resolve()

        while current_path != current_path.parent:  # Stop at root directory
            config_file = current_path / "config.yaml"
            if config_file.is_file():
                return current_path
            current_path = current_path.parent

        return None

    # Try finding config from current working directory
    root_dir = traverse(os.getcwd())

    # If not found, try from the directory of the script
    if root_dir is None:
        root_dir = traverse(Path(__file__).parent)

    if root_dir is None:
        raise FileNotFoundError("config.yaml not found in project directory")

    return str(root_dir / "config.yaml")


config = read_config()
