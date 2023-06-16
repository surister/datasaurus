import pytest

from datasaurus.core import models
from datasaurus.core.models.fields import StringField


@pytest.fixture
def dummy_model_class():
    class DummyModel(models.Model):
        field = StringField()

        class Meta:
            pass

    return DummyModel
