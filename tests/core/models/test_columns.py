from datasaurus.core.models.fields import Field, Fields

import polars as pl


def test_column_as_descriptors():
    """
    We test the behaviour of the column as a descriptor, both with column_name and without.
    """
    col_name = 'col'
    col_with_column_name = 'col2'
    other_col_name = 'other_col_name'

    class Dummy:
        col = Field()
        col2 = Field(column_name=other_col_name)

    dummy = Dummy()

    cls_col = getattr(Dummy, col_name)
    cls_col2 = getattr(Dummy, col_with_column_name)

    instance_col = getattr(dummy, col_name)
    instance_col2 = getattr(dummy, col_with_column_name)

    # Col without column_name
    assert hasattr(Dummy, col_name)
    assert isinstance(cls_col, type(pl.col(col_name)))
    assert col_name in str(cls_col)
    assert instance_col is None

    # Col with column name
    assert hasattr(Dummy, col_with_column_name)
    assert isinstance(cls_col2, type(pl.col(col_with_column_name)))
    assert other_col_name in str(cls_col2)
    assert instance_col2 is None

    # Test the descriptors __get__ and __set__ on instance as
    # they behave differently when used in cls vs instance.
    col_val = 1

    dummy.col = col_val
    dummy.col2 = col_val

    assert dummy.col == col_val
    assert dummy.col2 == col_val


def test_column_name():
    col = Field()
    col.__set_name__(None, 'col')

    col_1 = Field(column_name='column_name')
    col_1.__set_name__(None, 'col_1')

    assert col.name == 'col'
    assert col_1.name == 'col_1'
    assert col.get_column_name() == 'col'
    assert col_1.get_column_name() == 'column_name'


def test_columns():
    col_1 = Field()
    col_2 = Field(column_name='col_new')

    col_1.__set_name__(None, 'col_1')
    col_2.__set_name__(None, 'col_2')

    columns = Fields([col_1, col_2])

    assert col_1 in columns
    assert len(columns) == 2
    assert columns[0] == col_1

    col_3 = Field()
    col_3.__set_name__(None, 'col_3')

    columns_2 = Fields([col_3, ])
    columns.extend(columns_2)

    assert len(columns) == 3
    assert columns.get_model_columns() == ['col_1', 'col_2', 'col_3']
    assert columns.get_df_columns() == ['col_1', 'col_new', 'col_3']
