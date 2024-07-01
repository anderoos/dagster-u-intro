# fmt: off
from dagster import Definitions, load_assets_from_modules  # LOAD IN ASSET DEFINITIONS

from .assets import metrics, trips  # LOADS IN ASSETS
from .resources import database_resource  # READ IN RESOURCE VARIABLES

from .jobs import trip_update_job, weekly_update_job  # IMPORT JOBS
from .schedules import trip_update_schedule, weekly_update_schedule  # IMPORT CRON JOBS

# Objects
trip_assets = load_assets_from_modules([trips])
metric_assets = load_assets_from_modules([metrics])
all_schedules = [trip_update_schedule, weekly_update_schedule]
all_jobs = [trip_update_job, weekly_update_job]


defs = Definitions(
    assets=[*trip_assets, *metric_assets],
    resources={
        "database": database_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules
)