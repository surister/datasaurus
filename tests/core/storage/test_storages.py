import pytest

from datasaurus.core.storage import LocalStorage
from datasaurus.core.storage.format import FileFormat
from datasaurus.core.storage.storage import PostgresStorage, MysqlStorage, MariadbStorage, \
    SqliteStorage


@pytest.mark.parametrize('storage, format, storage_name', [
    (LocalStorage(path='/tmp'), FileFormat.CSV, LocalStorage),
    (LocalStorage(path='/tmp'), FileFormat.JSON, LocalStorage),
    (LocalStorage(path='/tmp'), FileFormat.PARQUET, LocalStorage),
    (LocalStorage(path='/tmp'), FileFormat.AVRO, LocalStorage),
    # (LocalStorage(path='/tmp'), FileFormat.EXCEL, LocalStorage),

    (SqliteStorage(path='/tmp/db.sqlite'), None, SqliteStorage),
    (PostgresStorage(username='user', password='password', host='localhost',
                     database='postgres'), None, PostgresStorage),
    (MysqlStorage(username='root', password='password', host='127.0.0.1',
                  database='test_db'), None, MysqlStorage),
    (MariadbStorage(username='root', password='password', host='127.0.0.1',
                    database='test_db'), None, MariadbStorage),

])
def test_write_to_storage(storage, format, dummy_dataframe, storage_name):
    """Test that all storages can write a dataframe"""
    dummy_filename = 'dummy'
    storage.write_file(df=dummy_dataframe, file_name=dummy_filename, format=format)
    assert storage.file_exists(dummy_filename, format=format)
