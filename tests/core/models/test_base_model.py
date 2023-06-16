import pytest

from datasaurus.core import models
from datasaurus.core.models.exceptions import MissingMeta, FieldNotExistsError
from datasaurus.core.models.fields import Field, Fields


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


def test_model_column_with_column_name_correctly_raises_when_set():
    class DummyModel(models.Model):
        field = Field(column_name='different_column_name')

        class Meta: pass

    value = 1

    with pytest.raises(FieldNotExistsError):
        DummyModel(different_column_name=value)


def test_model_fields_raises_if_not_exists(dummy_model_class):
    with pytest.raises(FieldNotExistsError):
        dummy_model_class(field_that_does_not_exist='text')


def test_model_has_correct_fields():
    class DummyModel(models.Model):
        field = Field(column_name='different_column_name')
        field2 = Field()

        class Meta: pass

    assert len(DummyModel.fields) == 2
    assert 'field' in DummyModel.fields and 'field2' in DummyModel.fields


def test_model_has_correct_meta_fields():
    class DummyModel(models.Model):
        field = Field(column_name='different_column_name')
        field2 = Field()

        class Meta: pass

    fields = DummyModel._meta.fields

    assert isinstance(fields, Fields)
    assert len(fields) == 2
    assert fields[0].column_name == 'different_column_name'

