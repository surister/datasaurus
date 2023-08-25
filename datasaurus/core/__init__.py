class classproperty(property):
    """
    It's the same as:

    ```
    @classmethod
    @property
    def my_class_property(cls):
        pass
    ```

    Note:
    It was added in python 3.9 but sequentially removed in 3.11.


    """
    def __get__(self, instance, owner):
        return self.fget(owner)
