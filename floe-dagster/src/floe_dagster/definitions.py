from dagster import Definitions

from .assets import floe_example_run


defs = Definitions(assets=[floe_example_run])
