- [x] Refactor ProxyStorage-Environment overall - OK
- [ ] Make write operations respect current model schema
- [ ] Fix how uri is generated
- [ ] Fix uri generation in SQLITEStorage + LOCALSTORAGE
- [x] Come up with new pattern for storage uri + readdf + write+df abstraction, probably a mixin could do: since most SQL databases 
  will share the same mixin
- [ ] Change string SQL queries to something that build SQL queries safely.
