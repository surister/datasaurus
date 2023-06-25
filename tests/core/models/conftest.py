import json
import pathlib
import uuid

import pytest

from datasaurus.core import models
from datasaurus.core.models import Model
from datasaurus.core.models.columns import StringColumn, IntegerColumn
from datasaurus.core.storage import StorageGroup, LocalStorage
from datasaurus.core.storage.format import FileFormat


@pytest.fixture
def dummy_model_class():
    class DummyModel(models.Model):
        column = StringColumn()

        class Meta:
            pass

    return DummyModel


@pytest.fixture
def model_with_local_data():
    tmp_dir = str(uuid.uuid4())[:4]
    file_path = f'/tmp/rand_{tmp_dir}'
    pathlib.Path(file_path).mkdir()
    with open(f'{file_path}/test_model.json', 'w') as f:
        f.write(
            json.dumps(
                {
                    "columns": [
                        {"name": "col2", "datatype": "Int64", "values": [1, 2, 3, 4]},
                        {"name": "col1", "datatype": "Utf8", "values": ['a', 'b', 'c', 'd']}
                    ]
                }
            )
        )

    class TestStorage(StorageGroup):
        local = LocalStorage(path=file_path)

    class TestModel(Model):
        col1 = StringColumn()
        col2 = IntegerColumn()

        class Meta:
            table_name = 'test_model'
            storage = TestStorage

    return TestModel
