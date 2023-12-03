import atexit
import multiprocessing
import signal

from pkg_resources import get_distribution, DistributionNotFound

from .shim import shark_load_shims
from .manifest import manifest

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    pass  # package is not installed

def shark_exit_handler(_sig=None, _frame=None):
    pid =  multiprocessing.process.current_process().pid
    print(f"{pid}: save")
    manifest.save()
    print(f"{pid}: close")
    manifest.close()
    print(f"{pid}: done")

# We need some multiprocessing story at some point, but not for now
pid =  multiprocessing.process.current_process().pid
print(f"{pid}: start (parent: {multiprocessing.parent_process()})")
if multiprocessing.parent_process() is None:
    print(f"{pid}: register")
    atexit.register(shark_exit_handler)
    signal.signal(signal.SIGTERM, shark_exit_handler)
    signal.signal(signal.SIGINT, shark_exit_handler)
    shark_load_shims()
