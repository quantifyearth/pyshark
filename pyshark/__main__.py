import sys

def execfile(filepath, globals=None, locals=None):
    if globals is None:
        globals = {}
    globals.update({
        "__file__": filepath,
        "__name__": "__main__",
    })
    with open(filepath, 'rb') as file:
        sys.argv = sys.argv[1:]
        exec(compile(file.read(), filepath, 'exec'), globals, locals) # pylint: disable=W0122

if len(sys.argv) > 1:
    execfile(sys.argv[1])
