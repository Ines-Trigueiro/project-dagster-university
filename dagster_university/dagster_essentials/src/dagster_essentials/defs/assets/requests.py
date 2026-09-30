import dagster as dg


@dg.asset
def requests(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
