import os
import argparse
import logging
import time

from ray._private.conda import (get_conda_activate_commands,
                                get_or_create_conda_env)

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()

parser.add_argument(
    "--conda-env-name",
    type=str,
    help="the name of an existing conda env to activate")

parser.add_argument(
    "--conda-yaml-path",
    type=str,
    help="the path to a conda environment yaml to install")


def setup(input_args):
    # remaining_args contains the arguments to the original worker command,
    # minus the python executable, e.g. default_worker.py --node-ip-address=...
    args, remaining_args = parser.parse_known_args(args=input_args)

    commands = []

    if args.conda_env_name:
        commands += get_conda_activate_commands(args.conda_env_name)
    elif args.conda_yaml_path:
        logger.error("Setting up conda yaml path")
        conda_env_name = get_or_create_conda_env(args.conda_yaml_path)
        print("sleeping")
        time.sleep(30)
        logger.error("Got conda env name")
        commands += get_conda_activate_commands(conda_env_name)
        logger.error("activated")

    commands += [" ".join(["exec python"] + remaining_args)]
    command_separator = " && "
    command_str = command_separator.join(commands)
    logger.error("command_str:")
    logger.error(command_str)
    logger.error("about to execvp")
    os.execvp("bash", ["bash", "-c", command_str])
