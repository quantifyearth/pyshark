
from .manifest import manifest

def read_file_shim(original_method, filename_arg_index=0):
	def shark_read_csv(*args, **kwargs):
		manifest.append_input(args[filename_arg_index])
		return original_method(*args, **kwargs)
	return shark_read_csv

def write_file_shim(original_method, filename_arg_index=0):
	def shark_read_csv(*args, **kwargs):
		manifest.append_output(args[filename_arg_index])
		return original_method(*args, **kwargs)
	return shark_read_csv

def load_pandas_shim() -> None:
	try:
		import pandas as pd
	except ImportError:
		return
	pd.read_csv = read_file_shim(pd.read_csv)
	pd.read_parquet = read_file_shim(pd.read_parquet)
	pd.DataFrame.to_csv = write_file_shim(pd.DataFrame.to_csv, 1)
	pd.DataFrame.to_parquet = write_file_shim(pd.DataFrame.to_csv, 1)

def load_yirgacheffe_shim() -> None:
	try:
		from yirgacheffe.layers import RasterLayer, VectorLayer, AreaLayer
	except ImportError:
		return
	RasterLayer.layer_from_file = read_file_shim(RasterLayer.layer_from_file, 1)
	VectorLayer.layer_from_file = read_file_shim(VectorLayer.layer_from_file, 1)


def shark_load_shims() -> None:
	load_pandas_shim()
	load_yirgacheffe_shim()
