import copy
import json

import pytest

from datasaurus import set_global_env
from datasaurus.core.models.exceptions import FormatNeededError
from datasaurus.core.storage.format import FileFormat

"""
The Model can create the dataframe from three different sources:
        1. Data from constructor - Model.from_dict({'column1': [1,2,3]})
        2. Data from calculation - Model.calculate_data()
        3. Data from Storage - Storage
"""

def test_df_creation_missing_columns(model_class_with_local_data):
    """
    We test that we allow model to have columns [a, b] and the origin df to be [a, b, c]
    (the final df will be [a, b] because of auto select)

    But we don't allow for: columns [a, b] and the origin df to be [b, c, d] we are missing 'a'
    """
    set_global_env('local')
    path = model_class_with_local_data._meta.storage.from_env.get_uri()

    model_class_with_local_data.df

    with open(f'{path}/test_model.json', 'w') as f:
        f.write(
            json.dumps(
                {
                    "columns": [
                        {"name": "col1", "datatype": "Utf8", "values": ['a', 'b', 'c', 'd']},
                    ]
                }
            )
        )
    with pytest.raises(ValueError):
        model_class_with_local_data.df

    with open(f'{path}/test_model.json', 'w') as f:
        f.write(
            json.dumps(
                {
                    "columns": [
                        {"name": "col1", "datatype": "Utf8", "values": ['a', 'b', 'c', 'd']},
                        {"name": "col2", "datatype": "Int64", "values": [1, 2, 3, 4]},
                        {"name": "col3", "datatype": "Int8", "values": [1, 2, 3, 4]},
                    ]
                }
            )
        )
    model_class_with_local_data.df


def test_df_creation_from_storage(model_class_with_local_data):
    # Todo Break this into smaller tests, per cases.
    """
    3. Data from Storage - Storage
    """
    import os
    os.environ['DATASAURUS_ENVIRONMENT'] = 'local'

    model = model_class_with_local_data
    default_meta = copy.copy(model_class_with_local_data._meta)

    def reset_meta(model):
        model._meta = copy.copy(default_meta)

    # Case 1: no format is given and cannot be inferred from table_name.
    model._meta.format = None

    with pytest.raises(FormatNeededError):
        model.df

    reset_meta(model)

    # Case 2: no format is given and can be inferred from table_name.
    model._meta.table_name += '.json'
    model.df

    reset_meta(model)

    # Case 3: no format is given, can be inferred from table_name, but it does not exist.
    model._meta.format = None
    model._meta.table_name += '.csv'
    with pytest.raises(ValueError):
        model.df

    reset_meta(model)

    # Case 4: format is given and can be inferred from table_name.
    model._meta.format = FileFormat.JSON
    model._meta.table_name += '.json'
    model.df

    reset_meta(model)

    # Case 5: format is given, but table name does not exist.
    model._meta.format = FileFormat.JSON
    model._meta.table_name += '.csv'
    model.df
    # Make sense that it works since read_json('file.csv') doesn't really care
    # about the suffix, remember that they are just information and not actual
    # indicator of the internal file format.
    reset_meta(model)

# Todo add more cases for other mixins
