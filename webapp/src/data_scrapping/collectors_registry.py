from src.data_scrapping.decorators import AddPages, RemoveDoneDates


class Registry:
    _registry = {}

    @classmethod
    def register(cls, name):
        def wrapper(subclass):
            cls._registry[name] = subclass
            return subclass

        return wrapper

    @classmethod
    def unregister(cls, name):
        assert name in cls._registry, f"Class '{name}' is not registered."
        del cls._registry[name]

    @classmethod
    def create(cls, name, *args, **kwargs):
        assert name in cls._registry, f"Class '{name}' is not registered."
        collector = cls._registry[name](*args, **kwargs)
        collector = AddPages(collector)
        collector = RemoveDoneDates(collector)
        return collector

    @classmethod
    def create_all(cls, *args, **kwargs):
        collectors = []
        for name in cls.list_registered():
            collectors.append(cls.create(name, *args, **kwargs))
        return collectors

    @classmethod
    def create_list(cls, name_list, *args, **kwargs):
        collectors = []
        for name in name_list:
            collectors.append(cls.create(name, *args, **kwargs))
        return collectors

    @classmethod
    def list_registered(cls):
        return list(cls._registry.keys())
