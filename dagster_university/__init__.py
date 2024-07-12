# fmt: off
from dagster import Definitions, load_assets_from_modules  # LOAD IN ASSET DEFINITIONS

from .assets import metrics, trips, requests  # LOADS IN ASSETS
from .resources import database_resource  # READ IN RESOURCE VARIABLES
from .jobs import trip_update_job, weekly_update_job, adhoc_request_job  # IMPORT JOBS
from .schedules import trip_update_schedule, weekly_update_schedule  # IMPORT CRON JOBS
from .sensors import adhoc_request_sensor

# Objects
trip_assets = load_assets_from_modules([trips])
metric_assets = load_assets_from_modules([metrics])
request_assets = load_assets_from_modules([requests])

all_schedules = [trip_update_schedule, weekly_update_schedule]
all_jobs = [trip_update_job, weekly_update_job, adhoc_request_job]
all_sensors = [adhoc_request_sensor]

defs = Definitions(
    assets=[*trip_assets, *metric_assets, *request_assets],
    resources={
       "database": database_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors
)
