Datasaurus is a Data Engineering framework written in Python 3.8, 3.9, 3.10 and 3.11

It is based in Polars and heavily influenced by Django.

Datasaurus offers an opinionated, feature-rich and powerful framework to help you write
data pipelines, ETLs or data manipulation programs.

[Documentation]() (TODO)
## It supports:
- ✅ Fully support read/write operations.
- ⭕ Not yet but will be implemented.
- 💀 Won't be implemented in the near future.

### Storages:
- Sqlite ✅
- PostgresSQL ✅
- MySQL ✅
- Mariadb ✅
- Local Storage ✅
- Azure blob storage ⭕
- AWS S3 ⭕


### Formats:
- CSV ✅
- JSON ✅
- PARQUET ✅
- EXCEL ✅
- AVRO ✅
- TSV ⭕
- SQL ⭕ (Like sql inserts)
- 
### Features:
- Delta Tables ⭕
- Field validations ⭕

## Simple example
```python
# settings.py 
from datasaurus.core.storage import PostgresStorage, StorageGroup, SqliteStorage
from datasaurus.core.models import StringColumn, IntegerColumn

# We set the environment that will be used.
os.environ['DATASAURUS_ENVIRONMENT'] = 'dev'

class ProfilesData(StorageGroup):
    dev = SqliteStorage(path='/data/data.sqlite')
    live = PostgresStorage(username='user', password='user', host='localhost', database='postgres')

    
# models.py
from datasaurus.core.models import Model, StringColumn, IntegerColumn

class ProfileModel(Model):
    id = IntegerColumn()
    username = StringColumn()
    mail = StringColumn()
    sex = StringColumn()

    class Meta:
        storage = ProfilesData
        table_name = 'PROFILE'

```

We can access the raw Polars dataframe with 'Model.df', it's lazy, meaning it will only load the
data if we access the attribute.

```py
>>> ProfileModel.df
shape: (100, 4)
┌─────┬────────────────────┬──────────────────────────┬─────┐
│ id  ┆ username           ┆ mail                     ┆ sex │
│ --- ┆ ---                ┆ ---                      ┆ --- │
│ i64 ┆ str                ┆ str                      ┆ str │
╞═════╪════════════════════╪══════════════════════════╪═════╡
│ 1   ┆ ehayes             ┆ colleen63@hotmail.com    ┆ F   │
│ 2   ┆ thompsondeborah    ┆ judyortega@hotmail.com   ┆ F   │
│ 3   ┆ orivera            ┆ iperkins@hotmail.com     ┆ F   │
│ 4   ┆ ychase             ┆ sophia92@hotmail.com     ┆ F   │
│ …   ┆ …                  ┆ …                        ┆ …   │
│ 97  ┆ mary38             ┆ sylvia80@yahoo.com       ┆ F   │
│ 98  ┆ charlessteven      ┆ usmith@gmail.com         ┆ F   │
│ 99  ┆ plee               ┆ powens@hotmail.com       ┆ F   │
│ 100 ┆ elliottchristopher ┆ wilsonbenjamin@yahoo.com ┆ M   │
└─────┴────────────────────┴──────────────────────────┴─────┘

```

We could now create a new model whose data is created from ProfileModel

```python
class FemaleProfiles(Model):
    id = IntegerField()
    profile_id = IntegerField()
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
        recalculate = 'if_no_data_in_storage'
        storage = ProfilesData
        table_name = 'PROFILE_FEMALES'
```
Et voilá! the columns will be auto selected from the column definitions (id, profile_id and email).

If we now call:
```python
FemaleProfiles.df
```

It will check if the dataframe exists in the storage and if it does not, it will 'calculate' it again
from calculate_data and save it to the Storage, this parameter can also be set to 'always'.


You can also move data to different environments or storages, making it easy to change formats or
move data around:

```python
FemaleProfiles.save(to=ProfilesData.live)
```

Effectively moving data from SQLITE (dev) to PostgreSQL (live), 

```python
# Can also change formats
FemaleProfiles.save(to=ProfilesData.otherenvironment, format=LocalFormat.JSON)
FemaleProfiles.save(to=ProfilesData.otherenvironment, format=LocalFormat.CSV)
FemaleProfiles.save(to=ProfilesData.otherenvironment, format=LocalFormat.PARQUET)
```
