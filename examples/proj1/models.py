from datasaurus.core.models import DataFormat, FileFormat
from datasaurus.core.models.base import Model
from datasaurus.core.storage.fields import StringField, IntegerField
from examples.proj1.settings import ProfilesData
import polars as pl


class ProfileModel(Model):
    id = IntegerField(auto=True)
    username = StringField()
    name = StringField()
    sex = StringField()
    address = StringField()
    mail = StringField()
    birthdate = StringField()

    class Meta:
        storage = ProfilesData
        table_name = 'PROFILE'


class FemaleProfiles(Model):
    id = IntegerField()
    profile_id = IntegerField(unique=True)
    mail = StringField()

    def calculate_data(self):
        return (
            ProfileModel.df
            .filter(ProfileModel.sex == 'F')
            .with_row_count('new_id')
            .with_columns(
                pl.col('new_id')
            )
            .with_columns(
                pl.col('id').alias('profile_id')
            )
        )

    class Meta:
        auto_select = True
        storage = ProfilesData
        table_name = 'PROFILE_FEMALES'
        format = FileFormat.JSON