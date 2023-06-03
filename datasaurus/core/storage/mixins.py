import pathlib

import polars as pl

from datasaurus.core.loggers import datasaurus_logger


def list_to_sql_columns(l: list[str]) -> str:
    """
    Transforms ["id", "username"...] into '(id, username)'
    """
    return str(l).replace('[', '').replace(']', '').replace("'", "")


class SQLStorageOperationsMixin:
    def read_file(self, file_name: str, columns: list):
        datasaurus_logger.debug(f'Trying to read {file_name}')
        query = f'SELECT {list_to_sql_columns(columns)} FROM "{file_name}"'
        datasaurus_logger.debug(f'query: {query}')
        datasaurus_logger.debug(f'uri: {self.get_uri()}')
        return pl.read_database(query, self.get_uri())

    def write_file(self, df: pl.DataFrame, file_name: str):
        if_exists = 'append' if self.file_exists(file_name) else 'replace'
        datasaurus_logger.debug(f'Attempting to write: {df}')
        datasaurus_logger.debug(
            f'Write configuration: { {"table_name": file_name, "connection_uri": self.get_uri(), "if_exists": if_exists} }'
        )
        df.write_database(
            table_name=file_name,
            connection_uri=self.get_uri(),
            if_exists=if_exists,
        )
        datasaurus_logger.debug(f'{file_name} written correctly.')

    def file_exists(self, table_name) -> bool:
        query = f'SELECT * FROM "{table_name}" LIMIT 1'
        datasaurus_logger.debug(f'Checking if file "{table_name}" exists, running query "{query}"')
        datasaurus_logger.debug(f'uri {self.get_uri()}')
        try:
            pl.read_database(query, self.get_uri())
        except RuntimeError as e:
            # This is very dirty, fixme
            return False
        return True


class LocalStorageOperationsMixin:
    def file_exists(self, file_name) -> bool:
        return pathlib.Path(self.get_uri()).exists()

    def write_file(self, data, file_name: str):
        # TODO see what to do with file_name here.
        return pathlib.Path(self.get_uri()).write_text(data)

    def read_file(self, file_name):
        return pathlib.Path(self.get_uri()).read_text()
