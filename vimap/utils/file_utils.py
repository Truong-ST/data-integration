import os

import yaml
import json
import re
import itertools

from typing import Text, Union, List, Dict


def write_yaml(data, file_path, **kwargs):
    if "encoding" in kwargs:
        encoding = kwargs['encoding']
    else:
        encoding = 'utf-8'
    with open(file_path, 'w', encoding=encoding) as pf:
        yaml.dump(data, pf, allow_unicode=True, default_flow_style=False, sort_keys=False)


def load_yaml(file_path, **kwargs):
    if "encoding" in kwargs:
        encoding = kwargs['encoding']
    else:
        encoding = 'utf-8'
    with open(file_path, 'r', encoding=encoding) as pf:
        return yaml.load(pf, Loader=yaml.SafeLoader)


def write_json(data, file_path, **kwargs):
    if "encoding" in kwargs:
        encoding = kwargs['encoding']
    else:
        encoding = 'utf-8'
    with open(file_path, 'w', encoding=encoding) as pf:
        json.dump(data, pf, ensure_ascii=False, indent=4)


def load_json(file_path, **kwargs):
    if "encoding" in kwargs:
        encoding = kwargs['encoding']
    else:
        encoding = 'utf-8'
    with open(file_path, 'r', encoding=encoding) as pf:
        return json.load(pf)

def open_file(file_path: str):
    with open(file_path, 'r') as file:
        data = file.readlines()
    return [example.rstrip() for example in data]


def write_file(data: list, file_path):
    with open(file_path, 'w') as file:
        for example in data:
            file.write(f"{example}\n")


