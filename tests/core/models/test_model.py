import pytest

import datasaurus
from datasaurus.core import models
from datasaurus.core.models import Model
from datasaurus.core.models.exceptions import MissingMetaError, ColumnNotExistsError
from datasaurus.core.models.columns import Column, Columns
from datasaurus.core.storage import StorageGroup, LocalStorage
from datasaurus.core.storage.format import FileFormat


def test_model_has_to_have_meta_class():
    with pytest.raises(MissingMetaError):
        class SomeModel(models.Model):
            pass


def test_model_can_be_instantiated():
    class SomeModel(models.Model):
        class Meta:
            pass

    SomeModel()


def test_model_columns_can_be_used(model_class_dummy):
    column_value = 'text'

    model = model_class_dummy(column=column_value)

    assert model.column == column_value


def test_model_column_with_column_name_correctly_raises_when_set():
    class DummyModel(models.Model):
        column = Column(name='different_column_name')

        class Meta:
            pass

    value = 1

    with pytest.raises(ColumnNotExistsError):
        DummyModel(different_column_name=value)


def test_model_column_raises_if_not_exists(model_class_dummy):
    with pytest.raises(ColumnNotExistsError):
        model_class_dummy(column_that_does_not_exists='text')


def test_model_has_correct_columns():
    class DummyModel(models.Model):
        column = Column(name='different_column_name')
        column2 = Column()

        class Meta:
            pass

    assert len(DummyModel.columns) == 2
    assert 'column' in DummyModel.columns and 'column2' in DummyModel.columns


def test_model_has_correct_meta_columns():
    class DummyModel(models.Model):
        column = Column(name='different_column_name')
        column2 = Column()

        class Meta:
            pass

    columns = DummyModel._meta.columns

    assert isinstance(columns, Columns)
    assert len(columns) == 2
    assert columns[0].column_name == 'different_column_name'


def test_model_resolves_environments_correctly():
    """
    We test _get_storage_or_default function since it is the function that resolves the storages
    for the model.

    """
    datasaurus.set_global_env('one')

    class FooStorage(StorageGroup):
        one = LocalStorage('/')
        two = LocalStorage('/')

    class FooModel(Model):
        class Meta:
            storage = FooStorage

    # Case 1: No storage, no environment.
    st = FooModel._get_storage_or_default(None)
    assert st == FooStorage.one

    # Case 2: No storage, yes environment.
    st = FooModel._get_storage_or_default(None, 'two')
    assert st == FooStorage.two

    # Case 2: GroupStorage yes, no environment.
    st = FooModel._get_storage_or_default(FooStorage)
    assert st == FooStorage.one

    # Case 3: GroupStorage yes, environment yes.
    st = FooModel._get_storage_or_default(FooStorage, 'two')
    assert st == FooStorage.two

    # Case 4: Storage yes, environment no.
    st = FooModel._get_storage_or_default(FooStorage.one,)
    assert st == FooStorage.one

    # Case 5: Storage yes, environment yes.
    st = FooModel._get_storage_or_default(FooStorage.one, 'two')
    assert st == FooStorage.two


def test_model_enforces_dtypes(model_class_with_local_data):
    datasaurus.set_global_env('local')
    model_class_with_local_data._meta.format = FileFormat.JSON
