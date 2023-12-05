# PyShark: Python bindings for ARK

This is a prototype library, doing a mock up of what the shark run time would do eventually, but doing it inside the Python environment just to provide us with a proof of concept to demonstrate the value (or lack thereof) of keeping provenance information with results.

Currently this only works on Linux, as it messes with the internals of multiprocessing, which behaves differently on different platforms.

## Usage:

Either just import pyshark:

```python
import pyshark # pylint: disable=unused-import
```

Or run your script via the pyshark module:

```shell
$ python3 -m pyshark ./my_script_name.py
```

## What does it do:

As you write out files from your python script, they will have provenance information attached to them either via an extended file attribute or as dot file along side, depending on whether your target file system supports extended attributes or not.

The data is a JSON encoded manifest containing information allowing you to understand how a result file was generated. For example it attemtps to work out:

* Git repository of the source code (assuming it exists in the current working directory)
* Files opened
* Files written
* Platform information
* Python modules loaded
* When the file was generated, and how long the execution was

This isn't exchaustive, but it's a pragmatic start to allowing you understand the provenance of any result file you're looking at.

## How does it do it:

PyShark is currently a gross hack to just prove a point. In an ideal world, Shark containers would do this, so that it worked no matter whether you used Python, R, gdal_translate, etc., but we just want to get a sense of how this metadata would be used, so this python library just does the same thing by shimming common file open methods and hooking into multiprocessing, to try track what source data was used to generate a result.

## What doesn't it do:

This sounds like a taint tracking library or a security tool: IT IS NOT THESE THINGS. This is just a proof of concept of a research idea, and so is far from watertight!
