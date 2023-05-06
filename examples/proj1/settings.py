from datasaurus.core.storage import LocalStorage, define_storage, ScopedStorage, Environments


class MyDataFolder(ScopedStorage):
    live = define_storage(
        storage=LocalStorage(file_name='datalive.json', path='/suush'),
    )
    ci = define_storage(
        environment='Continuous Integration',
        storage=LocalStorage(file_name='dataci.json', path='/suush'),
    )


class PenTest(ScopedStorage):
    pentest1 = define_storage(
        storage=LocalStorage(file_name='datalive.json', path='/suush'),
    )
    pentest2 = define_storage(
        environment='Pentest2 server',
        storage=LocalStorage(file_name='dataci.json', path='/suush'),
    )


live = MyDataFolder.live
ci = MyDataFolder.ci
# from_env = MyDataFolder.from_env
print(live)
print(ci)
# print(from_env)
print(MyDataFolder.environments)

test = define_storage(
    storage=LocalStorage(file_name='datalive.json', path='/suush'),
)

print(test)

print(PenTest.environments)