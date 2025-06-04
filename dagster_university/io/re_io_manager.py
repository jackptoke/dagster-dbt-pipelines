import json
from typing import Any

from dagster import IOManager, OutputContext, InputContext

from dagster_university.assets import constants


class ReIOPayload(object):
    def __init__(self, data: [dict], filepath: str):
        self.data = data
        self.filepath = filepath


class ReIOManager(IOManager):
    def handle_output(self, context: OutputContext, data_content: ReIOPayload) -> None:
        # context.log.info(f"ReIOManager partition def: {context.asset_partition_keys}")
        load_data(data=data_content.data, filepath=data_content.filepath)
        # storage_path = f"/data/output/{context.asset_key.path[-1]}"
        # with open(storage_path, "w") as f:
        #     f.write(str(data_content))

    def load_input(self, context: InputContext) -> Any:
        raise NotImplementedError()
        # storage_path = f"/data/input/{context.asset_key.path[-1]}"
        # with open(storage_path, "r") as f:
        #     return f.read()


def load_data(data, filepath: str):
    constants.ensure_directory_exists(filepath)
    with open(filepath, "w+") as f:
        json.dump(data, f, indent=4)
