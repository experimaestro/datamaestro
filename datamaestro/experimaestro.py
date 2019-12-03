import os 
import logging

# If experimaestro is installed, use it... otherwise, use dummy classes
try:
    if os.environ.get("NO_EXPERIMAESTRO", 0) == "1":
        raise ModuleNotFoundError

    from experimaestro import *
    logging.debug("Using real experimaestro")
except ModuleNotFoundError:
    # Use dummy classes    
    logging.debug("Using dummy experimaestro")
    class Typename:
        def __init__(self, name):
            self.name = name

    class XPMInfo():
        def __init__(self):
            self.arguments = {}

    class Argument:
        def __init__(self, name, type=None, help="", ignored=False, default=None, required=True):
            self.help = help
            self.required = required
            self.default = default
            self.name = name
        def __call__(self, t):
            assert self.name not in t.__xpm__.arguments
            t.__xpm__.arguments[self.name] = self
            return t

    class Type:
        def __init__(self, typename):
            self.typename = typename

        def __call__(self, t):
            def init(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)
                for key, value in self.__xpm__.arguments.items():
                    if key not in kwargs and value.default is not None:
                        setattr(self, key, value.default)

            t.__xpm__ = XPMInfo()
            t.__init__ = init
            return t

XPMNS = Typename("xpm")