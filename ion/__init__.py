# Make sure the init code in ioninit (e.g. logging settings) executes before
# anything else in the system
from .core import ioninit
from .core import ionconst

__version__ = ionconst.VERSION
