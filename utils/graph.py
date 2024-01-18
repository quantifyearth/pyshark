import json
import os
import sys
from urllib.parse import urlparse

import graphviz
import xattr

domain_nodes = {}

def plot(upstream, program, inputs, dot):

    dot.node(program+upstream, program, shape="box")
    dot.edge(program+upstream, upstream)

    for input in inputs:
        try:
            _, name = os.path.split(input["path"])
        except KeyError:
            name = input["url"]
            parts = urlparse(name)
            try:
                l = domain_nodes[parts.netloc]
            except KeyError:
                l = []
            l.append(str(name.__hash__()))
            domain_nodes[parts.netloc] = l

        node_id = str(name.__hash__())

        dot.node(node_id, name)
        dot.edge(node_id, program+upstream)

        try:
            history = input["history"]
            plot(node_id, history["args"][0], history["inputs"], dot)
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
    program = history["args"][0]
    plot(source, program, inputs, dot)

    for domain in domain_nodes:
        nodes = domain_nodes[domain]
        with dot.subgraph(name=f"cluster_{domain}") as cluster:
            cluster.attr(label=domain)
            cluster.attr(style="dotted")
            for node in nodes:
                cluster.node(node)

    dot.render("/tmp/blah.gv")

if __name__ == "__main__":
    main()
