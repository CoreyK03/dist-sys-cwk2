import json
import logging
import random
import time

import azure.functions as func
import numpy as np
import pandas as pd
from azure.functions.decorators.core import DataType

TANKS_NUM = 50

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def generate_tank_data(tank_id: int, current_time: float) -> dict:
    temp_ideal = 26.0
    ph_ideal = 7.0
    oxygen_ideal = 8.0

    simulated_temp = temp_ideal + 2.4 * np.sin(current_time / 100 + 2 * tank_id) + random.uniform(-0.2, 0.2)
    simulated_ph = ph_ideal + 0.52 * np.sin(current_time / 100 + 2 * tank_id) + random.uniform(-0.1, 0.1)
    simulated_oxygen = oxygen_ideal + 1.5 * np.sin(current_time / 100 + 2 * tank_id) + random.uniform(-0.3, 0.3)
    simulated_ammonia = 0.22 * np.abs(np.sin(current_time / 100) + 2 * tank_id) + random.uniform(0, 0.05)

    return {
        "tank_id": tank_id,
        "temperature": simulated_temp,
        "ph": simulated_ph,
        "oxygen": simulated_oxygen,
        "ammonia": simulated_ammonia,
        "timestamp": current_time,
    }


@app.function_name("GenerateData")
@app.sql_output(
    arg_name="sensorData",
    command_text="[dbo].[SensorData]",
    connection_string_setting="SqlConnectionString",
)
@app.schedule(schedule="*/30 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def sensor_data_generator(myTimer: func.TimerRequest, sensorData: func.Out[func.SqlRowList]) -> None:
    current_time = time.time()

    rows = []
    for i in range(TANKS_NUM):
        data = generate_tank_data(i + 1, current_time)
        rows.append(func.SqlRow(data))

    logging.info("Data Simulated")
    sensorData.set(func.SqlRowList(rows))


@app.function_name("ProcessData")
@app.sql_trigger(arg_name="sensorDataChange", table_name="SensorData", connection_string_setting="SqlConnectionString")
@app.sql_input(
    arg_name="sensorData",
    command_text="SELECT * FROM [dbo].[SensorData] WHERE timestamp > (DATEDIFF(SECOND, '1970-01-01 00:00:00', GETUTCDATE()) - 600);",
    command_type="Text",
    connection_string_setting="SqlConnectionString",
)
@app.generic_output_binding(
    arg_name="aggSensorData",
    type="sql",
    CommandText="dbo.AggregatedSensorData",
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING,
)
def sensor_data_trigger(
    sensorDataChange: str, sensorData: func.SqlRowList, aggSensorData: func.Out[func.SqlRowList]
) -> None:
    data = list(map(lambda r: json.loads(r.to_json()), sensorData))

    df = pd.DataFrame(data)

    aggregated_data = df.groupby("tank_id").agg(
        {
            "temperature": ["min", "max", "mean"],
            "ph": ["min", "max", "mean"],
            "oxygen": ["min", "max", "mean"],
            "ammonia": ["min", "max", "mean"],
        }
    )

    aggregated_data.columns = ["_".join(col) for col in aggregated_data.columns.values]
    aggregated_data["timestamp"] = time.time()

    rows = []

    for tank_id in aggregated_data.index:
        row = aggregated_data.loc[tank_id].to_dict()
        row["tank_id"] = tank_id
        rows.append(func.SqlRow(row))

    logging.info("Uploading aggregated data...")

    aggSensorData.set(rows)


@app.route(route="sensor_data")
@app.sql_input(
    arg_name="sensorData",
    command_text="SELECT t1.* FROM [dbo].[SensorData] t1 JOIN (SELECT tank_id, MAX(timestamp) AS max_timestamp FROM [dbo].[SensorData] GROUP BY tank_id) t2 ON t1.tank_id = t2.tank_id AND t1.timestamp = t2.max_timestamp;",
    command_type="Text",
    connection_string_setting="SqlConnectionString",
)
def sensor_data(req: func.HttpRequest, sensorData: func.SqlRowList) -> func.HttpResponse:
    logging.info("Function invoked.")

    data = list(map(lambda r: json.loads(r.to_json()), sensorData))

    logging.info("Returning data.")

    return func.HttpResponse(json.dumps({"results": data}), status_code=200)


@app.route(route="aggregated_sensor_data")
@app.sql_input(
    arg_name="aggSensorData",
    command_text="SELECT * FROM [dbo].[AggregatedSensorData] WHERE timestamp > (DATEDIFF(SECOND, '1970-01-01 00:00:00', GETUTCDATE()) - 3600);",
    command_type="Text",
    connection_string_setting="SqlConnectionString",
)
def aggregated_sensor_data(req: func.HttpRequest, aggSensorData: func.SqlRowList) -> func.HttpResponse:
    logging.info("Function invoked.")

    data = list(map(lambda r: json.loads(r.to_json()), aggSensorData))

    logging.info("Returning data.")

    return func.HttpResponse(json.dumps({"results": data}), status_code=200)
