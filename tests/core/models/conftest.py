import pytest

from datasaurus.core import models
from datasaurus.core.models.fields import StringColumn


@pytest.fixture
def dummy_model_class():
    class DummyModel(models.Model):
        field = StringColumn()

        class Meta:
            pass

    return DummyModel
