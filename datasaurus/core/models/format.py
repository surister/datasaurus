from enum import Enum, auto


class DataFormat:
    """
    Dummy class just for type hinting.
    """
    pass


class FormatNotSet(DataFormat):
    def __contains__(self, item):
        return False

    def __str__(self):
        return 'FormatsNotSet'


class FileFormat(DataFormat, Enum):
    @property
    def name(self) -> str:
        return super().name.lower()

    JSON = auto()
    CSV = auto()
    PARQUET = auto()
    EXCEL = auto()
    AVRO = auto()
