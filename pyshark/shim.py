
from .manifest import manifest

def pandas_read_file_shim(original_method):
	def shark_read_csv(*args, **kwargs):
		manifest.append_input(args[0])
		return original_method(*args, **kwargs)
	return shark_read_csv

def load_pandas_shim() -> None:
	try:
		import pandas as pd
	except ImportError:
		return
	pd.read_csv = pandas_read_file_shim(pd.read_csv)
	pd.read_parquet = pandas_read_file_shim(pd.read_parquet)

def shark_load_shims() -> None:
	load_pandas_shim()
