import pytest

from datasaurus.core import models
from datasaurus.core.models.exceptions import MissingMeta, FieldNotExistsError


def test_model_has_to_have_meta_class():
    with pytest.raises(MissingMeta):
        class SomeModel(models.Model):
            pass


def test_model_can_be_instantiated():
    class SomeModel(models.Model):
        class Meta:
            pass

    SomeModel()


def test_model_fields_can_be_used(dummy_model_class):
    field_value = 'text'

    model = dummy_model_class(field=field_value)

    assert model.field == field_value


def test_model_fields_raises_if_not_exists(dummy_model_class):
    with pytest.raises(FieldNotExistsError):
        dummy_model_class(field_that_does_not_exist='text')
