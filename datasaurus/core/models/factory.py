from datasaurus.core.models.columns import Column


class factory_attribute(Column):
    def __init__(self, default_value_or_lambda):
        self.default_value_or_lambda = default_value_or_lambda
        super().__init__()

    def evaluate(self):
        if callable(self.default_value_or_lambda):
            return self.default_value_or_lambda()
        return self.default_value_or_lambda


class ModelFactory:
    class Meta:
        model = None

    @classmethod
    def get_columns(cls):
        return {k: v for k, v in cls.__dict__.items() if isinstance(v, factory_attribute)}

    @classmethod
    def validate_columns(cls, attrs):
        columns = Columns(attrs.values())
        if columns != cls.Meta.model._meta.columns:
            raise Exception('Cols are not the same')

    @classmethod
    def create_rows(cls, n_rows: int):
        factory_cols = cls.get_columns()
        cls.validate_columns(factory_cols)

        rows = []
        for _ in range(n_rows):
            values_dict = {k: v.evaluate() for k, v in factory_cols.items()}
            rows.append(
                cls.Meta.model(**values_dict)
            )
        return rows
