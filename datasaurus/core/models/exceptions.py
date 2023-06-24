class MissingMetaError(Exception):
    """Raise when Model does not have a Meta class."""


class FormatNotSupportedByModelError(Exception):
    """Raise when a Model does not support a format, and we are trying to force it"""


class FormatNeededError(Exception):
    """Raise when the Model always needs a format and it was not provided"""


class ColumnNotExistsError(Exception):
    """Raises when column does not exist in Model and its being provided"""
