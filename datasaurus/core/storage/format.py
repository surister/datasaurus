from enum import Enum, auto, EnumMeta


class DataFormat:
    """
    Dummy class just for type hinting.
    """

    def __getitem__(cls, name):
        print('hey')
        return cls._member_map_[name.upper()]


class FormatNotSet(DataFormat):
    def __contains__(self, item):
        return False

    def __str__(self):
        return 'FormatNotSet'


class LowerIndexedMeta(EnumMeta):
    def __getitem__(cls, name):
        return cls._member_map_[name.upper()]


class LowerIndexedEnum(Enum, metaclass=LowerIndexedMeta):
    pass


class FileFormat(DataFormat, LowerIndexedEnum):
    JSON = auto()
    CSV = auto()
    PARQUET = auto()
    EXCEL = auto()
    AVRO = auto()

    @property
    def name(self) -> str:
        return super().name.lower()

    @property
    def suffix(self) -> str:
        return f'.{super().name.lower()}'
