from datasaurus.core.models.base import Model, IntegerField, StringField
from examples.proj1.settings import ProfilesData


class ProfileModel(Model):
    id = IntegerField(auto=True)
    username = StringField()
    name = StringField()
    sex = StringField()
    address = StringField()
    mail = StringField()
    birthdate = StringField()

    class Meta:
        __storage__ = ProfilesData
        __table_name__ = 'PROFILE'


class FemaleProfiles(Model):
    id = IntegerField(auto=True)
    profile_id = IntegerField(unique=True)
    mail = StringField()

    def calculate_data(self):
        return ProfileModel.df.filter(ProfileModel.sex == 'F')

    class Meta:
        __recalculate__ = True
        __from__ = ProfileModel
        __storage__ = ProfilesData
        __table_name__ = 'PROFILE_FEMALES'
