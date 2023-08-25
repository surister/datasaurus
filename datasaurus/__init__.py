import os


def set_global_env(environment_name: str) -> None:
    os.environ['DATASAURUS_ENVIRONMENT'] = environment_name
