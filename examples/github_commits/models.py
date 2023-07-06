from datasaurus.core.models import Model
from datasaurus.core.models.columns import StringColumn, DateTimeColumn, IntegerColumn
from datasaurus.core.storage import FileFormat
from examples.github_commits.settings import CommitsStorage


class GithubCommit(Model):
    commit = StringColumn()
    author = StringColumn()
    date = DateTimeColumn(format='%a %b %d %H:%M:%S %Y %z', utc=True)
    message = StringColumn()
    repo = StringColumn()

    class Meta:
        storage = CommitsStorage
        table_name = 'full'
        format = FileFormat.PARQUET


class Repository(Model):
    id = IntegerColumn(auto_add_id=True)
    name = StringColumn()

    class Meta:
        recalculate = False
        storage = CommitsStorage
        table_name = 'repositories'
        format = FileFormat.PARQUET

    def calculate_data(self) -> 'polars.DataFrame':
        return (
            GithubCommit.df
            .select(GithubCommit.repo).unique()
            .rename({'repo': 'name'})
        )


class Author(Model):
    id = IntegerColumn()
    name = StringColumn(unique=True)
    email = StringColumn()

    class Meta:
        storage = CommitsStorage
        table_name = 'authors'
        format = FileFormat.PARQUET


class CommitMessage(Model):
    id = IntegerColumn()
    hash = IntegerColumn()
    repository_id = IntegerColumn()
    author_id = IntegerColumn()
    message = StringColumn()
    message_len = IntegerColumn()

    class Meta:
        storage = CommitsStorage
        table_name = 'commit_messages'
        format = FileFormat.PARQUET
