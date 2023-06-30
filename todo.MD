# Completed
- [x] Refactor ProxyStorage-Environment overall
- [x] Make write operations respect current model schema
- [x] Improve logging.
- [x] Come up with new pattern for storage uri + readdf + write+df abstraction, probably a mixin
  could do: since most SQL databases will share the same mixin
- [x] Fix how uri is generated
- [x] Make it so we can store newly generated data in different storage
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
- [x] Add data types enforcement / casting from the Columns objects
- [x] Add more fields types
- [x] Change everything to `columns` (We now use the words fields/columns in several parts of the code)
- [x] Add more options to Columns depending on the column Type, like 'unique',  'column_name'
- [x] Make read/write operations to respect Fields options, like 'column_name'
- [x] Create factory for models (like factory-boy + faker)

# Models
- [ ] Come up with a pattern for 1:n and n:n model transformations.
- [ ] Change _from_dict constructor into something more generic such as _from_data
- [ ] [Bug] When you create a df from_dict, if you access to model.df twice, the second time raises and error
- [ ] Cannot write from one mixin to another (create df from one storage and save it to another if it's of different type)
- [ ] Make factory be able to create dataframe as well
- [ ] Give the ability to factory to use multiprocessing to create larges amount of data quick
- [ ] Create unit tests for factory (once the three above are done) 

# Columns
- [ ] Add unit tests for enforce_dtype
- [ ] If auto_id_add is added, see how to add another option, ex: 'default' so we can pass stuff like lambda x: uuid4
- [ ] Add auto_id_add

# Storages + IO
- [ ] Add save modes such as: append, overwrite, 'delta'?
- [ ] Change string SQL queries to something that build SQL queries safely.
- [ ] Unit test dataframe creation (3)
- [ ] Add support for ndjson format
- [ ] Investigate and choose the default mysql/mariadb driver https://docs.sqlalchemy.org/en/20/dialects/mysql.html (mysqlclient can be used)
to read but not to write.

# General Stuff
- [ ] Create custom exceptions, we are currently using Exception and ValueError in many places. 
- [ ] Add CI pipeline with unittests and linting.

# Nice things to have
- [ ] Support Azure blob storage read/write
- [ ] Create utility that can give you Model code from inferred data like django inspectdb
- [ ] Ability to create dummy data from a given Model, (maybe implement with faker boy)

# For the future
- [ ] Support delta tables
- [ ] Fully support streaming
- [ ] Validations and transformations with 2 different patterns.
      1. We support inline basic validations/transformation like:
      even_field = forms.IntegerField(validators=[validate_even])
      2. Like https://django-filter.readthedocs.io/en/stable/guide/usage.html
        where rules are outside the model, in a different class and linked in the
        Model's Meta.
