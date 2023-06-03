from enum import Enum, auto


class DataFormat:
    pass


class FormatNotSet(DataFormat):
    def __contains__(self, item):
        return False

    def __str__(self):
        return 'FormatsNotSet'


class LOCAL_FORMAT(DataFormat, Enum):
    @property
    def name(self) -> str:
        return super().name.lower()

    JSON = auto()
    CSV = auto()
    PARQUET = auto()
    EXCEL = auto()
    AVRO = auto()
