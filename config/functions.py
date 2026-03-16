import yaml

YAML_CONFIG_PATH= "./fiap-dep-payments-pipeline/config/settings.yaml"

def load_config(path: str = YAML_CONFIG_PATH) -> dict:
    with open(path, "r") as file:
        return yaml.safe_load(file)