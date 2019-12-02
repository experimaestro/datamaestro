import logging

# If experimaestro is installed, use it... otherwise, use dummy classes
try:
    from experimaestro import *
    logging.debug("Using real experimaestro")
except ModuleNotFoundError:
    # Use dummy classes    
    logging.debug("Using dummy experimaestro")
    class Typename:
        def __init__(self, name):
            self.name = name

    class Argument:
        def __init__(self, name, type, help="", default=None, required=True):
            self.help = help
            self.required = required
        def __call__(self, t):
            return t

    class Type:
        def __init__(self, typename):
            self.typename = typename

        def __call__(self, t):
            def init(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            t.__init__ = init
            return t

XPMNS = Typename("xpm")