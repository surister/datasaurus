import pytest

from datasaurus.core import models
from datasaurus.core.models.exceptions import MissingMeta


def test_model_has_to_have_meta_class():
    with pytest.raises(MissingMeta):
        class SomeModel(models.Model):
            pass
