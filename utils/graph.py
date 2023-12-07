import json
import os
import sys

import graphviz
import xattr

def plot(upstream, inputs, dot):
    for input in inputs:
        try:
            _, name = os.path.split(input["path"])
        except KeyError:
            name = input["url"]
        node_id = str(name.__hash__())
        dot.node(node_id, name)
        dot.edge(node_id, upstream)

        try:
            history = input["history"]
            plot(node_id, history["inputs"], dot)
        except KeyError:
            pass


def main() -> None:
    source = sys.argv[1]

    dot = graphviz.Digraph(comment=source)

    _, node_filename = os.path.split(source)
    dot.node(source, node_filename)

    history = None
    xattr_info = xattr.xattr(source)
    if 'user.shark' in xattr_info:
        history = json.loads(xattr_info['user.shark'].decode("utf-8"))
    else:
        path, filename = os.path.split(source)
        sidefilename = os.path.join(path, f".{filename}.shark")
        try:
            with open(sidefilename, "r") as sidefile:
                history = json.loads(sidefile.read())
        except OSError:
            pass
    if history is None:
        return

    inputs = history["inputs"]
    plot(source, inputs, dot)

    dot.render("/tmp/blah.gv")

if __name__ == "__main__":
    main()
