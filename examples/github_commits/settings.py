from datasaurus.core.storage import StorageGroup, LocalStorage


class CommitsStorage(StorageGroup):
    local = LocalStorage(path='/mydata')

