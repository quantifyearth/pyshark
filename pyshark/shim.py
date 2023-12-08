import builtins
import os
from multiprocessing import pool, context, parent_process

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

def load_gdal_shim() -> None:
    try:
        from osgeo import gdal
    except ImportError:
        return
    gdal.Open = read_file_shim(gdal.Open)
    gdal.Driver.Create = write_file_shim(gdal.Driver.Create)

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


def python_close_shim(original_method, descriptor):
    def python_close(*args, **kwargs):
        manifest.close_fd(descriptor)
        res = original_method(*args, **kwargs)
        return res
    return python_close

def python_open_shim(original_method):
    def python_open(*args, **kwargs):
        filename = args[0]
        try:
            mode = args[1]
        except IndexError:
            mode = kwargs.get("mode", "r")
        fileobject = original_method(*args, **kwargs)
        if ('w' in mode) or ('x' in mode) or ('a' in mode) or ('+' in mode):
            manifest.append_output(filename, fileobject.fileno())
        if ('r' in mode) or ('+' in mode):
            manifest.append_input(filename, fileobject.fileno())
        fileobject.close = python_close_shim(fileobject.close, fileobject.fileno())
        return fileobject
    return python_open

def load_python_shim() -> None:
    builtins.open = python_open_shim(builtins.open)

def worker_get_shim(original_method):
    def shark_worker_queue_get(*args, **kwargs):
        manifest.snapshot()
        return original_method(*args, **kwargs)
    return shark_worker_queue_get

def worker_put_shim(original_method):
    def shark_worker_queue_put(*args, **kwargs):
        manifest.save()
        manifest.child_flush()
        manifest.restore()
        return original_method(*args, **kwargs)
    return shark_worker_queue_put

def map_worker_shim(original_method):
    def shark_pool_worker(*args, **kwargs):
        args[0].get = worker_get_shim(args[0].get)
        args[1].put = worker_put_shim(args[1].put)
        return original_method(*args, **kwargs)
    return shark_pool_worker

def map_shim(original_method):
    def shark_pool_map(*args, **kwargs):
        res = original_method(*args, **kwargs)
        manifest.parent_flush()
        return res
    return shark_pool_map

def load_multiprocessing_pool_shim() -> None:
    pool.worker = map_worker_shim(pool.worker)
    pool.Pool.map = map_shim(pool.Pool.map)

def main_process_queue_get_shim(original_method):
    def shark_main_queue_get(*args, **kwargs):
        if parent_process() is None:
            manifest.parent_flush()
        return original_method(*args, **kwargs)
    return shark_main_queue_get

def main_process_queue_put_shim(original_method):
    def shark_main_queue_put(*args, **kwargs):
        if parent_process() is not None:
            manifest.child_flush()
        return original_method(*args, **kwargs)
    return shark_main_queue_put

def process_start_shim(original_method):
    def shark_start_shim(*args, **kwargs):
        caller_args = args[0]._args  # pylint: disable = W0212
        # now we are post fork in parent - child doesn't
        # return from this call
        for arg in caller_args:
            try:
                arg.get = main_process_queue_get_shim(arg.get)
                arg.put = main_process_queue_put_shim(arg.put)
            except AttributeError:
                pass
        return original_method(*args, **kwargs)
    return shark_start_shim

def process_join_shim(original_method):
    def shark_join_shim(*args, **kwargs):
        res = original_method(*args, **kwargs)
        if parent_process() is None:
            manifest.parent_flush()
        return res
    return shark_join_shim

def load_multiprocesing_process_shim() -> None:
    context.Process.start = process_start_shim(context.Process.start)
    context.Process.join = process_join_shim(context.Process.join)

def os__exit_shim(original_method) -> None:
    # This is unfortunate, as _exit is meant to be the
    # fast track exit don't call anything, but it's also
    # the way child processes exit
    def shark__exit(*args, **kwargs):
        if parent_process() is not None:
            manifest.save()
            manifest.close()
        return original_method(*args, **kwargs)
    return shark__exit

def load_os__exit_shim() -> None:
    os._exit = os__exit_shim(os._exit)


def requests_get_shim(original_method):
    def shark_get_shim(*args, **kwargs):
        try:
            url = args[0]
            manifest.append_url_input(url)
        except IndexError:
            pass
        return original_method(*args, **kwargs)
    return shark_get_shim

def load_requests_shim() -> None:
    try:
        import requests
    except ImportError:
        return
    requests.get = requests_get_shim(requests.get)

def shark_load_shims() -> None:
    manifest.builtin_open = builtins.open
    load_pandas_shim()
    load_gdal_shim()
    load_geopandas_shim()
    load_yirgacheffe_shim()
    load_requests_shim()
    load_python_shim()
    load_multiprocessing_pool_shim()
    load_multiprocesing_process_shim()
    load_os__exit_shim()
