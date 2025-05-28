from typing import Any

from dagster import IOManager, io_manager, OutputContext, InputContext


class ReIOManager(IOManager):
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        storage_path = f"/data/output/{context.asset_key.path[-1]}"
        with open(storage_path, "w") as f:
            f.write(str(obj))

    def load_input(self, context: InputContext) -> Any:
        storage_path = f"/data/input/{context.asset_key.path[-1]}"
        with open(storage_path, "r") as f:
            return f.read()


