import dagster as dg


@dg.sensor(target=None)
def sensors(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    return dg.SensorResult()
