# Completed
- [x] Refactor ProxyStorage-Environment overall
- [x] Make write operations respect current model schema
- [x] Improve logging.
- [x] Come up with new pattern for storage uri + readdf + write+df abstraction, probably a mixin
  could do: since most SQL databases will share the same mixin
- [x] Fix how uri is generated
- [x] Make it so we can store newly generated data in different storages
- [x] Investigate whether ModelBase metaclass' function columns, calculate_data, exists,
  ~~ensure_exists~~ ...etc should be out of the metaclass
- [x] Allow for a model df to be created from a dictionary
- [x] Improve class Meta implementation.
- [x] Make it so we can store new generated data in different FORMATS
- [x] Create new constructor to build a model from a dictionary
- [x] Raise exception when no Meta is present.
- [x] Allow for inheritance where fields and Meta options are gotten from bases.
- [x] Improve Model.save api: allow always for table_name override.
- [x] Improve Model.save api: allow always for environment (as string) override.
- [x] Add mysql support
- [x] Add column_name option to Columns
- [x] Add data types enforcement / casting from the Columns
- [x] Add more fields types
- [x] Change everything to `columns` (We now use the words fields/columns in several parts of the code)
- [x] Add more options to Columns depending on the column Type, like 'unique',  'column_name'
- [x] Make read/write operations to respect Fields options, like 'column_name'
- [x] Create factory for models (like factory-boy + faker)
- [x] Make factory be able to create dataframe as well
- [x] Give the ability to factory to use multiprocessing to create larges amount of data quick
- [x] Change _from_dict constructor into something more generic such as _from_data
- [x] Ability to create dummy data from a given Model, (maybe implement with faker boy)
- [x] [Bug] When you create a df from_dict, if you access to model.df twice, the second time raises
  and error (not a bug, its expected behaviour)
- [x] Make Model.from_dict constructor raise exception when None is passed
- [x] Add auto_id_add
- [x] Perhaps recalculate = 'Always', 'Once', 'If not storage data' where 'if not storage data' is default could be interesting for entities flow
- [x] From storages, change 'environment init param' into 'environment_name' for better clarity
- [x] Model: 'missing_columns' could accept columns like this:
df.columns: ['col1', 'col2', 'col3'], models: ['col1', 'col2'] because there will be times when if the source data 
has too many columns, and we just don't want to write them.
- [x] Remove options in Columns such as 'unique', I tried in a ETL and did not like it at all working with it.
- [x] Make columns importable from datasaurus.core.models

# Models
- [ ] Come up with a pattern for 1:n and n:n model transformations.
- [ ] Cannot write from one mixin to another (create df from one storage and save it to another if it's of different type)
- [ ] Create unit tests for factory (once the three above are done) 

# Columns
- [ ] Add validations.

# Storages + IO
- [ ] Change string SQL queries to something that build SQL queries safely.
- [ ] Add support for ndjson format
- [ ] Investigate and choose the default mysql/mariadb driver https://docs.sqlalchemy.org/en/20/dialects/mysql.html (mysqlclient can be used)
to read but not to write.
- [ ] Add support to modify read/write options, like Models.with_options(environment=whatever).save() (That'd modify the read from that's defined in Meta)
- [ ] Add support to read/write compressed files like gz/zip ..etc

# General Stuff
- [ ] Create custom exceptions, we are currently using Exception and ValueError in many places. 
- [ ] Add CI pipeline with unittests and linting.
- [ ] Investigate a pattern for ETL/Pipeline creation
- [ ] Investigate how are we going to manage dependencies between models
- [ ] Add more debugging logs
- [ ] Add header to debug logs, ex: 
datasaurus - DEBUG - [ModelFactory] Execution strategy will be PythonMultiprocessing(processes=24)
datasaurus - DEBUG - [ModelIO] Trying to read Model from storage {storageinfo}
datasaurus - DEBUG - [StorageIO] Executing SQL {...} to see if table exists

# Things where unittests are missing
- [ ] Unit test model inheritance.
- [ ] Add unit tests for enforce_dtype
- [ ] Unit test dataframe creation (1)

# Nice things to have
- [ ] Support Azure blob storage read/write
- [ ] Support S3 storage read/write
- [ ] Create utility that can give you Model code from inferred data like django inspectdb
- [ ] Unit tests features like chispa
- [ ] Automatic data lineage from instrospection from model inheritance, simple model calculate_data and transformation pattern.

# For the future
- [ ] Support delta tables
- [ ] Fully support streaming
- [ ] Validations and transformations with 2 different patterns.
      1. We support inline basic validations/transformation like:
      even_field = forms.IntegerField(validators=[validate_even])
      2. Like https://django-filter.readthedocs.io/en/stable/guide/usage.html
        where rules are outside the model, in a different class and linked in the
        Model's Meta.
