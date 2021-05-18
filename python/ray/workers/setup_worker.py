import argparse
import logging
# TODO(architkulkarni): move to ray.utils
from ray.serve.utils import import_attr

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description=(
        "Set up the environment for a Ray worker and launch the worker."))

parser.add_argument(
    "--worker-setup-hook",
    type=str,
    help="the module path to a Python function to run to set up the "
    "environment for a worker and launch the worker.")

args, remaining_args = parser.parse_known_args()

setup = import_attr(args.worker_setup_hook)

logger.error("about to set up remaining args")
setup(remaining_args)
logger.error("set up remaining args")
