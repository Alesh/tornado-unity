""" Config classes
"""
import importlib.machinery


class ModuleConfig(object):
    """ Config container based on module
    """

    def __init__(self, default, defined=None, ):
        def load_module(filename, name):
            loader = importlib.machinery.SourceFileLoader(name, filename)
            return loader.load_module()

        self._defined = None
        if defined is not None:
            self._defined = load_module(defined, defined)
        self._default = load_module(default, default)

    def __getattr__(self, name):
        if name.startswith('_'):
            return self.__getattribute__(name)
        else:
            if hasattr(self._defined, name):
                return getattr(self._defined, name)
            return getattr(self._default, name)
