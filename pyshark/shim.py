
from .manifest import manifest

def read_file_shim(original_method, filename_arg_index=0):
    def shark_read_file(*args, **kwargs):
        manifest.append_input(args[filename_arg_index])
        return original_method(*args, **kwargs)
    return shark_read_file

def write_file_shim(original_method, filename_arg_index=0):
    def shark_read_file(*args, **kwargs):
        manifest.append_output(args[filename_arg_index])
        return original_method(*args, **kwargs)
    return shark_read_file

def load_pandas_shim() -> None:
    try:
        import pandas as pd
    except ImportError:
        return
    pd.read_csv = read_file_shim(pd.read_csv)
    pd.read_parquet = read_file_shim(pd.read_parquet)
    pd.DataFrame.to_csv = write_file_shim(pd.DataFrame.to_csv, 1)
    pd.DataFrame.to_parquet = write_file_shim(pd.DataFrame.to_csv, 1)

def load_geopandas_shim() -> None:
    try:
        from geopandas import gpd
    except ImportError:
        return
    gpd.read_file = read_file_shim(gpd.read_file)

def yirgacheffe_write_file_shim(original_method):
    def yirgacheffe_empty_raster(*args, **kwargs):
        if "filename" in kwargs:
            manifest.append_output(kwargs["filename"])
        return original_method(*args, **kwargs)
    return yirgacheffe_empty_raster

def load_yirgacheffe_shim() -> None:
    try:
        from yirgacheffe.layers import RasterLayer, VectorLayer
    except ImportError:
        return
    RasterLayer.layer_from_file = read_file_shim(RasterLayer.layer_from_file)
    VectorLayer.layer_from_file = read_file_shim(VectorLayer.layer_from_file)
    RasterLayer.empty_raster_layer = yirgacheffe_write_file_shim(RasterLayer.empty_raster_layer)
    RasterLayer.empty_raster_layer_like = yirgacheffe_write_file_shim(RasterLayer.empty_raster_layer_like)

def python_open_shim(original_method):
    def python_open(*args, **kwargs):
        filename = args[0]
        mode = kwargs.get("mode", "r")
        if ('w' in mode) or ('x' in mode) or ('a' in mode) or ('+' in mode):
            manifest.append_output(filename)
        if ('r' in mode) or ('+' in mode):
            manifest.append_input(filename)
        return original_method(*args, **kwargs)
    return python_open

def load_python_shim() -> None:
    import builtins
    builtins.open = python_open_shim(builtins.open)

def shark_load_shims() -> None:
    load_pandas_shim()
    load_geopandas_shim()
    load_yirgacheffe_shim()
    load_python_shim()
