import os
from abc import ABC, abstractmethod
from typing import Callable, List, Dict

from datasaurus.core.loggers import datasaurus_logger
from datasaurus.core.models import Model
from datasaurus.core.models.columns import Column, Columns


class factory_attribute(Column):
    def __init__(self, default_value_or_lambda):
        self.default_value_or_lambda = default_value_or_lambda
        super().__init__()

    def evaluate(self):
        if callable(self.default_value_or_lambda):
            return self.default_value_or_lambda()
        return self.default_value_or_lambda


class ExecutionStrategy(ABC):
    @abstractmethod
    def execute(self, iterations: int, func: Callable, **extra_options):
        pass


class PythonNormal(ExecutionStrategy):
    """Normal Python loop execution"""

    def execute(self, iterations: int, func: Callable, **extra_options):
        return [func() for _ in range(iterations)]


class PythonMultiprocessing(ExecutionStrategy):
    MP_THRESHOLD = 10_000
    """
    Python multiprocessing execution, processes and chunk_size can be tweaked.

    Parameters
    ----------
    processes : int
        The processes that will be used, if None Python will use all the available cores.

    chunk_size : int
        The approximate amount of chunks that the iterable will be chopped into and passed
        into the process to execute.
    
    Notes
    -----
    Using multiprocessing for low amounts of iterations has a negative impact in performance,
    since the overhead is bigger than the time saved. 
    
    That's why there is MP_THRESHOLD, while its value might not actually be the point
    where multiprocessing makes computationally sense it is at least a start to mitigate the issue.
    """

    def __init__(self, processes: int = None, chunk_size: int = None):
        self.processes = processes or os.cpu_count()
        self.chunk_size = chunk_size

    def get_chunksize(self, n) -> int:
        """
        Returns an approximate of a good chunksize, giving every process the same amount of
        tasks.

        Parameters
        ----------
        n : int
            The total amount of iterations.
        """

        return n // os.cpu_count() if n > self.MP_THRESHOLD else 1

    def execute(self, iterations: int, func: Callable, **extra_options):
        def _parallelize_obj_creation(n, func):
            import multiprocessing as mp
            with mp.Pool(self.processes) as pool:
                res = pool.imap(func, range(n), chunksize=self.chunk_size or self.get_chunksize(n))
                return list(res)

        return _parallelize_obj_creation(iterations, func)

    def __repr__(self):
        return f'{self.__class__.__qualname__}(processes={self.processes})'

    def __str__(self):
        return f'{self.__class__.__qualname__}(processes={self.processes})'


class ModelFactory:
    class Meta:
        model = None
        execution_strategy = None

    @classmethod
    def get_execution_strategy(cls) -> ExecutionStrategy:
        return (
            cls.Meta.execution_strategy
            if hasattr(cls.Meta, 'execution_strategy')
            else PythonNormal()
        )

    @classmethod
    def get_columns(cls) -> Dict[str, factory_attribute]:
        return {k: v for k, v in cls.__dict__.items() if isinstance(v, factory_attribute)}

    @classmethod
    def validate_columns(cls, attrs) -> None:
        columns = Columns(attrs.values())
        if columns != cls.Meta.model._meta.columns:
            raise Exception('Cols are not the same')

    @classmethod
    def create_rows(cls, n_rows: int) -> List[Model]:
        factory_cols = cls.get_columns()
        cls.validate_columns(factory_cols)

        ex = cls.get_execution_strategy()

        return ex.execute(
            iterations=n_rows,
            func=lambda: cls.Meta.model(
                **cls.generate_one_row()
            ),
        )

    @classmethod
    def generate_one_row(cls, _=None) -> dict:
        return {k: v.evaluate() for k, v in cls.get_columns().items()}

    @classmethod
    def create_df(cls, n_rows: int) -> Model:
        datasaurus_logger.debug(f'Creating df for model {cls.Meta.model}')
        factory_cols = cls.get_columns()
        cls.validate_columns(factory_cols)

        ex = cls.get_execution_strategy()

        datasaurus_logger.debug(f'Execution strategy will be {ex}')

        rows = ex.execute(
            iterations=n_rows,
            func=cls.generate_one_row
        )

        return cls.Meta.model.from_data(rows)
