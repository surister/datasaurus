from datasaurus.core.models import FileFormat
from datasaurus.core.models.base import Model
from datasaurus.core.models.fields import StringColumn, IntegerField, DateTimeColumn, DateColumn
from examples.proj1.settings import ProfilesData
import polars as pl


class ProductData(Model):
    id = IntegerField()
    product_name = StringColumn()
    sku = StringColumn()
    stock = StringColumn()

    class Meta:
        storage = ProfilesData
        table_name = 'PRODUCTS'


class EligibleUsersOver18(Model):
    """
    Users that have ever bought something over 18
    (So we can send them sex toys spam/ads legally)
    """
    id = IntegerField()
    profile_id = IntegerField()

    class Meta:
        storage = ProfilesData
        table_name = 'EligibleUsersOver18'


class OrderData(Model):
    id = IntegerField()
    profile_id = StringColumn()
    sku = StringColumn()

    class Meta:
        storage = ProfilesData
        table_name = 'OrderData'

    # def calculate_data(self) -> 'polars.DataFrame':
    #     return (
    #         pl.DataFrame()
    #         .with_columns(
    #             ProfileModel.df.select('id').alias('profile_id')
    #         )
    #         .with_row_count('id')
    #         .with_columns(
    #             pl.col('id')
    #         )
    #         .with_columns(
    #             ProductData.df.select('sku')
    #         )
    #
    #     )


class ProfileModel(Model):
    id = IntegerField(auto=True)
    username = StringColumn()
    name = StringColumn()
    sex = StringColumn()
    address = StringColumn()
    mail = StringColumn()
    birthdate = DateColumn()

    class Meta:
        storage = ProfilesData
        table_name = 'PROFILE'


class FemaleProfiles(Model):
    id = IntegerField()
    profile_id = IntegerField(unique=True)
    mail = StringColumn()

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
        recalculate = True
        storage = ProfilesData
        table_name = 'PROFILE_FEMALES'


class FemaleProfileWithNum(FemaleProfiles):
    num = IntegerField()


class TestModel(FemaleProfileWithNum):
    test = StringColumn()

    class Meta:
        table_name = 'TestModel'
