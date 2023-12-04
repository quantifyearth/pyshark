import atexit
import os
import platform
import signal

from pkg_resources import get_distribution, DistributionNotFound

from .shim import shark_load_shims
from .manifest import manifest

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    pass  # package is not installed

def shark_exit_handler(_sig=None, _frame=None):
    manifest.save()
    manifest.close()

if platform.uname().system != "Linux":
    raise ImportWarning("Multiprocessing support only works on linux, may cause bad behaviour otherwhere.")

atexit.register(shark_exit_handler)
signal.signal(signal.SIGTERM, shark_exit_handler)
signal.signal(signal.SIGINT, shark_exit_handler)
shark_load_shims()
