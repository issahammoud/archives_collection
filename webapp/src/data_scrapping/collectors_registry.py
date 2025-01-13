from src.data_scrapping.decorators import AddPages, EliminateRedundancy


class Registry:
    _registry = {}

    @classmethod
    def register(cls):
        def wrapper(subclass):
            cls._registry[subclass.__name__] = subclass
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
        collector = EliminateRedundancy(collector)
        return collector

    @classmethod
    def create_all(cls, *args, **kwargs):
        collectors = []
        for name in cls.list_registered():
            collectors.append(cls.create(name, *args, **kwargs))
        return collectors
    
    @classmethod
    def list_registered(cls):
        return list(cls._registry.keys())
    
    