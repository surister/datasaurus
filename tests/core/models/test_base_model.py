import pytest

from datasaurus.core import models
from datasaurus.core.models.exceptions import MissingMetaError, ColumnNotExistsError
from datasaurus.core.models.columns import Column, Columns


def test_model_has_to_have_meta_class():
    with pytest.raises(MissingMetaError):
        class SomeModel(models.Model):
            pass


def test_model_can_be_instantiated():
    class SomeModel(models.Model):
        class Meta:
            pass

    SomeModel()


def test_model_columns_can_be_used(dummy_model_class):
    column_value = 'text'

    model = dummy_model_class(column=column_value)

    assert model.column == column_value


def test_model_column_with_column_name_correctly_raises_when_set():
    class DummyModel(models.Model):
        column = Column(name='different_column_name')

        class Meta: pass

    value = 1

    with pytest.raises(ColumnNotExistsError):
        DummyModel(different_column_name=value)


def test_model_column_raises_if_not_exists(dummy_model_class):
    with pytest.raises(ColumnNotExistsError):
        dummy_model_class(column_that_does_not_exists='text')


def test_model_has_correct_columns():
    class DummyModel(models.Model):
        column = Column(name='different_column_name')
        column2 = Column()

        class Meta: pass

    assert len(DummyModel.columns) == 2
    assert 'column' in DummyModel.columns and 'column2' in DummyModel.columns


def test_model_has_correct_meta_columns():
    class DummyModel(models.Model):
        column = Column(name='different_column_name')
        column2 = Column()

        class Meta: pass

    columns = DummyModel._meta.columns

    assert isinstance(columns, Columns)
    assert len(columns) == 2
    assert columns[0].column_name == 'different_column_name'

