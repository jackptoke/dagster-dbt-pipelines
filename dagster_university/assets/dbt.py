from typing import Any, Optional
from typing import Mapping

import dagster as dg
from dagster import AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from dagster_university.project import dbt_project


# Customise how Dagster should translate DBT metadata
class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    # Extract the asset name, helping Dagster to identify their dependencies
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_type = dbt_resource_props['resource_type']
        name = dbt_resource_props['name']
        if resource_type in ["source", "staging", "cleansed", "fct"]:
            return dg.AssetKey(name)
        return super().get_asset_key(dbt_resource_props)

    # Extract group name of the DBT assets if specified,
    # otherwise a default value is returned, i.e. DBT_ASSETS
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        # resource_type = dbt_resource_props['resource_type']
        meta = dict(dbt_resource_props['meta'])
        if len(meta.keys()) == 0 or "dagster" not in list(meta.keys()):
            return "DBT_ASSETS"
        group_name = meta['dagster']["group"] if "group" in list(meta["dagster"].keys()) else "DBT_ASSETS"
        return group_name


# Automatically, build the DBT assets at every refresh
@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def dbt_analytics(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    # yield from dbt.cli(["build"], context=context, raise_on_error=False).stream()
    # yield from dbt.cli(["test"], context=context).stream()
    dbt_cli_invocation = dbt.cli(["build"], context=context)
    for dbt_event in dbt_cli_invocation.stream_raw_events():
        for dagster_event in dbt_event.to_default_asset_events(
                manifest=dbt_cli_invocation.manifest,
                dagster_dbt_translator=dbt_cli_invocation.dagster_dbt_translator,
                context=dbt_cli_invocation.context,
                target_path=dbt_cli_invocation.target_path,
        ):
            context.log.info(f"Dagster event: {dagster_event}")
            yield dagster_event
