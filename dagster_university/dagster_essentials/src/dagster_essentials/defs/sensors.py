# src/dagster_essentials/defs/sensors.py
import dagster as dg
import os
import json

from dagster_essentials.defs.jobs import adhoc_request_job

@dg.sensor(
    job=adhoc_request_job
)
def adhoc_request_sensor(context: dg.SensorEvaluationContext):
    # the directory the sensor will observe
    PATH_TO_REQUESTS = os.path.join(
        os.path.dirname(__file__),
        "../../../",
        "data/requests",
    )

    # define the cursor
    # context argument stores the cursor used to manage the state 
    # of what the sensor has already looked at. 
    # The cursor may or may not exist, depending on if the sensor has previously had a tick run.
    # To accommodate for this, we check if context.cursor exists and if it does, 
    # convert its string value into JSON
    previous_state = json.loads(context.cursor) if context.cursor else {}
    # We also initialize the current_state to an empty object, 
    # which we’ll use to override the cursor after it reads through the directory.
    current_state = {}

    # used to store the new requests we want to create runs for
    runs_to_request = []

    # Uses os.listdir to iterate through the data/requests directory, 
    # looking at every JSON file, and seeing if it’s been updated
    # or looked at it before in previous_state
    for filename in os.listdir(PATH_TO_REQUESTS):
        file_path = os.path.join(PATH_TO_REQUESTS, filename)
        if filename.endswith(".json") and os.path.isfile(file_path):
            last_modified = os.path.getmtime(file_path)

            current_state[filename] = last_modified

            # if the file is new or has been modified since the last run, add it to the request queue
            if filename not in previous_state or previous_state[filename] != last_modified:
                with open(file_path, "r") as f:
                    request_config = json.load(f)

                    # Creates a RunRequest for the file 
                    # if it's been updated or a report hasn’t been run before
                    runs_to_request.append(dg.RunRequest(
                        # Constructs a unique run_key, 
                        # which includes the name of the file 
                        # and when it was last modified
                        run_key=f"adhoc_request_{filename}_{last_modified}",
                        # Passes the run_key into the RunRequest's configuration 
                        # using the run_config argument. 
                        # By using the adhoc_request key, 
                        # you specify that the adhoc_request asset 
                        # should use the config provided.
                        run_config={
                            "ops": {
                                "adhoc_request": {
                                    "config": {
                                        "filename": filename,
                                        **request_config
                                    }
                                }
                            }
                        }
                    ))
    # Sensors expect a SensorResult returned, 
    # which contains all the information for the sensor, 
    # such as which runs to trigger and what the new cursor is.
    return dg.SensorResult(
        run_requests=runs_to_request,
        cursor=json.dumps(current_state),
        )

