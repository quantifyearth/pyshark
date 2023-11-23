import atexit
import signal

from pkg_resources import get_distribution, DistributionNotFound

from .shim import shark_load_shims
from .manifest import manifest

try:
	__version__ = get_distribution(__name__).version
except DistributionNotFound:
	pass  # package is not installed

def shark_exit_handler():
	manifest.save(None)

atexit.register(shark_exit_handler)
signal.signal(signal.SIGTERM, shark_exit_handler)
signal.signal(signal.SIGINT, shark_exit_handler)
shark_load_shims()
