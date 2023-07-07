import json
import pathlib
import uuid

import polars
import pytest

from datasaurus.core import models
from datasaurus.core.models import Model
from datasaurus.core.models.columns import StringColumn, IntegerColumn
from datasaurus.core.storage import StorageGroup, LocalStorage
from datasaurus.core.storage.format import FileFormat


def create_tmp_dir():
    """
    Creates a writable/readable random temporal directory in /tmp/.

    """
    tmp_dir = str(uuid.uuid4())[:4]
    file_path = f'/tmp/rand_{tmp_dir}'
    pathlib.Path(file_path).mkdir()
    return file_path


@pytest.fixture
def model_class_dummy():
    class DummyModel(models.Model):
        column = StringColumn()

        class Meta:
            pass

    return DummyModel


@pytest.fixture
def model_class_with_local_data():
    tmp_dir = create_tmp_dir()

    with open(f'{tmp_dir}/test_model.json', 'w') as f:
        f.write(
            json.dumps(
                {
                    "columns": [
                        {"name": "col1", "datatype": "Utf8", "values": ['a', 'b', 'c', 'd']},
                        {"name": "col2", "datatype": "Int64", "values": [1, 2, 3, 4]},
                        {"name": "col3", "datatype": "Int8", "values": [1, 2, 3, 4]},
                    ]
                }
            )
        )

    class TestStorage(StorageGroup):
        local = LocalStorage(path=tmp_dir)

    class TestModel(Model):
        col1 = StringColumn()
        col2 = IntegerColumn()

        class Meta:
            table_name = 'test_model'
            storage = TestStorage
            format = FileFormat.JSON
            # There is no format on purpose because the tests this is used in
            # overrides/tests different formats.

    return TestModel


@pytest.fixture
def model_class_with_local_data_all_dtypes():
    tmp_dir = create_tmp_dir()

    with open(f'{tmp_dir}/test_model.json', 'w') as f:
        f.write(
            json.dumps(
                {
                    "columns": [
                        {"name": "col1", "datatype": "Decimal", "values": [1, 2, 3, 4]},
                        {"name": "col2", "datatype": "Float32", "values": [1, 2, 3, 4]},
                        # {"name": "col3", "datatype": "Int8", "values": [1, 2, 3, 4]},
                        # {"name": "col4", "datatype": "UInt8", "values": [1, 2, 3, 4]},
                        # {"name": "col5", "datatype": "Utf8", "values": ['1', '2', '3', '4']},
                    ]
                }
            )
        )

    class TestStorage(StorageGroup):
        local = LocalStorage(path=tmp_dir)

    class TestModel(Model):
        col1 = IntegerColumn()
        col2 = IntegerColumn()
        col3 = IntegerColumn()
        col4 = IntegerColumn()
        col5 = IntegerColumn()

        class Meta:
            table_name = 'test_model'
            storage = TestStorage
            format = FileFormat.JSON

    return TestModel
