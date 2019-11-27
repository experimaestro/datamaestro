# If experimaestro is installed, use it... otherwise, use dummy classes
try:
    from experimaestro import *
except ModuleNotFoundError:
    # Use dummy classes
    raise NotImplementedError("Implement experimaestro base classes")


XPMNS = Typename("xpm")