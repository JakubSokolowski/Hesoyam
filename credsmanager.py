import json
from pathlib import Path

credentials_path = '/home/jakub/dev/repos/Hesoyam/credentials.json'
config_path = str(Path(__file__).parent) + '/config/'


def get_credentials(site_name: str):
    with open(credentials_path) as f:
        creds = json.load(f)
    return {
        'reddit': creds['reddit'],
    }.get(site_name, {})


def get_config(site_name: str):
    path = config_path + site_name + '.json'
    with open(path) as f:
        return json.load(f)


def update_config(new_config, site_name: str):
    path = config_path + site_name + '.json'
    with open(path, 'w') as f:
        return json.dump(new_config, f, indent=4)
