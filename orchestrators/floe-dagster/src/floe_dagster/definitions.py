from dagster import Definitions

from .assets import floe_run


defs = Definitions(assets=[floe_run])
