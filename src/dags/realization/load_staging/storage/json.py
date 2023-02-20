import json
import pathlib

from .base import BaseStorage
from .model import Workflow
from .utils import MyEncoder


class JsonFileStorage(BaseStorage):

    def __init__(self, path_to_file: str, etl_key: str):
        self.file_path = pathlib.Path(path_to_file)
        if not self.file_path.exists():
            self.save_state(Workflow(workflow_key=etl_key))
        if not self.file_path.is_file():
            raise Exception("Указанный, как файл, json хранилище состояния, это папка!")

    def save_state(self, state: Workflow):
        with open(self.file_path, 'w', encoding='utf8') as f:
            json.dump(state.dict(), f, cls=MyEncoder)

    def retrieve_state(self) -> Workflow:
        with open(self.file_path, 'r', encoding='utf8') as f:
            state = json.load(f)
        return Workflow(**state)
