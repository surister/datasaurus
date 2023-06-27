class classproperty(property):
    def __get__(self, instance, owner):
        return self.fget(owner)
