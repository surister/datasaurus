# Completed
- [x] Refactor ProxyStorage-Environment overall - OK
- [x] Make write operations respect current model schema
- [x] Improve logging.
- [x] Come up with new pattern for storage uri + readdf + write+df abstraction, probably a mixin
  could do: since most SQL databases
  will share the same mixin
- [x] Fix how uri is generated
- [x] Make it so we can store new generated data in different storage
- [x] Investigate whether ModelBase metaclass's function columns, calculate_data, exists,
  ensure_Exists.. should be out of the metaclass
- [x] Allow for a model df to be created from a dictionary
- [x] Improve class Meta implementation.
- [x] Make it so we can store new generated data in different FORMATS
- [x] Create new constructor to build df from a dictionary
- [x] Raise exception when no Meta is present.
- [x] Allow for inheritance where fields and Meta options are gotten from bases.
- [x] Improve Model.save api: allow always for table_name override.
- [x] Improve Model.save api: allow always for environment (as string) override.
- [x] Add mysql support

# Models
- [ ] Come up with a pattern for 1:n and n:n model transformations.

# Fields
- [ ] Add column_name option to Fields
- [ ] Add more fields types

# Storages + IO
- [ ] Make read/write operations to respect Fields options, like 'unique', 'column_name'
- [ ] Add save modes such as: append, overwrite, 'delta'?
- [ ] Change string SQL queries to something that build SQL queries safely.
- [ ] Make tests where we do all possible IO combinations with all mixin implementations
- [ ] Implement ndjson format

# General Stuff
- [ ] Create custom exceptions, we are currently using Exception and ValueError everywhere. 

# Nice things to have
- [ ] Support Azure blob storage read/write
- [ ] Create utility that can give you Model code from inferred data like django inspectdb
- [ ] Ability to create dummy data from a given Model, (maybe implement with faker boy)

# For the future
- [ ] Support delta tables
- [ ] Fully support streaming
