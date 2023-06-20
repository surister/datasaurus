import pytest

from datasaurus.core import models
from datasaurus.core.models.columns import StringColumn


@pytest.fixture
def dummy_model_class():
    class DummyModel(models.Model):
        column = StringColumn()

        class Meta:
            pass

    return DummyModel
