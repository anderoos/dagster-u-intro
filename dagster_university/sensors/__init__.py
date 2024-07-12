from dagster import (
    RunRequest,  # USED TO TRIGGER RUNS
    SensorResult,  # RETURNS CURSOR INFORMATION
    sensor,  # THE DECORATOR THAT EVALUATES CONDITION
    SensorEvaluationContext  # PROVIDES METADATA FOR SENSOR
)

import os
import json

from ..jobs import adhoc_request_job


@sensor(
    job=adhoc_request_job
)
def adhoc_request_sensor(context: SensorEvaluationContext) -> None:
    # __File__ navigates to current dir and gets name.
    # ../.. and the path moves up two levels and moves into requests folder
    PATH_TO_REQUESTS = os.path.join(os.path.dirname(__file__), "../../", "data/requests")
    # DEFINE CURSOR
    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    runs_to_request = []

    for filename in os.listdir(PATH_TO_REQUESTS):  # Iterate through files
        file_path = os.path.join(PATH_TO_REQUESTS, filename)
        if filename.endswith(".json") and os.path.isfile(file_path):  # Check if ends in JSON
            last_modified = os.path.getmtime(file_path)  # Check time and add to current state

            current_state[filename] = last_modified

            # if the file is new or has been modified since the last run, add it to the request queue
            if filename not in previous_state or previous_state[filename] != last_modified:
                with open(file_path, "r") as f:
                    request_config = json.load(f)

                    runs_to_request.append(RunRequest(
                        run_key=f"adhoc_request_{filename}_{last_modified}",
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
    return SensorResult(
        run_requests=runs_to_request,
        cursor=json.dumps(current_state)
    )
