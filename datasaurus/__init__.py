import os


class classproperty(property):
    def __get__(self, instance, owner):
        return self.fget(owner)


def set_global_env(environment_name: str) -> None:
    os.environ['DATASAURUS_ENVIRONMENT'] = environment_name
