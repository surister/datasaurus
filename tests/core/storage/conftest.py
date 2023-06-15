import pytest

import polars

from datasaurus.core.storage import StorageGroup, LocalStorage


@pytest.fixture
def storage_group_with_one_storage_per_environment(tmp_path):
    class storage_group(StorageGroup):
        local = LocalStorage(path=str(tmp_path))

    return storage_group


@pytest.fixture
def dummy_dataframe():
    data = {
        'id': [1, 2, 3, 4],
        'profile_id': [1, 2, 3, 4],
        'mail': [1, 2, 3, 4],
    }
    return polars.DataFrame(data)
