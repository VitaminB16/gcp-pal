import importlib


class LazyLoader:
    """Lazy loader that defers the import of a module or class until it's actually used."""

    def __init__(self, module_name, class_name=None):
        self.module_name = module_name
        self.class_name = class_name
        self.module = None

    def __getattr__(self, name):
        if self.module is None:
            self.module = importlib.import_module(self.module_name)
        return getattr(self.module, name)

    def __class__(self):
        return self.module

    def __call__(self, *args, **kwargs):
        """Allow the loader to be called and delegate to the imported class."""
        cls = self._load()
        return cls(*args, **kwargs)

    def _load(self):
        if self.module is None:
            self.module = importlib.import_module(self.module_name)
        if self.class_name:
            return getattr(self.module, self.class_name)
        return self.module
