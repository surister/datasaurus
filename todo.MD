- [x] Refactor ProxyStorage-Environment overall - OK
- [x] Make write operations respect current model schema
- [x] Improve logging.
- [x] Come up with new pattern for storage uri + readdf + write+df abstraction, probably a mixin
  could do: since most SQL databases
  will share the same mixin
- [x] Fix how uri is generated
- [ ] Change string SQL queries to something that build SQL queries safely.
- [ ] Investigate whether ModelBase metaclass's function columns, calculate_data, exists,
  ensure_Exists.. should be out of the metaclass
- [ ] Support delta tables
- [ ] Make write operations respect __overwrite__, it should always be True, 
- [ ] Create pattern Workflow/Transformation for Multimodel
- [x] Make it so we can store new generated data in different storage
- [ ] Make it so we can store new generated data in different FORMATS
- [ ] Make read/write operations to respect Fields options, like 'unique', 'column_name'
- [ ] Add column_name option to Fields