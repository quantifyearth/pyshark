import hashlib
import json
import os
import platform
import sys
from datetime import datetime, timezone
from typing import Any,Dict

import git
import pkg_resources

class Manifest:
    def __init__(self):
        self.start = datetime.now(timezone.utc)
        self.inputs = []
        self.outputs = set()

    def append_input(self, filename: str) -> None:
        if filename in [x['path'] for x in self.inputs]:
            return
        hash = hashlib.sha1()
        with self.builtin_open(filename, "rb") as f:
            while chunk := f.read(1024 * hash.block_size):
                hash.update(chunk)

        self.inputs.append({'path': filename, 'sha': hash.hexdigest()})

    def append_output(self, filename: str) -> None:
        self.outputs.add(filename)

    @staticmethod
    def get_python_env() -> Dict[str,Any]:
        return {
            "version": sys.version,
            "packages": {p.project_name:p.version for p in pkg_resources.working_set}, # pylint: disable=E1133
        }

    @staticmethod
    def get_platform() -> Dict[str,str]:
        uname = platform.uname()
        return {
            "system": uname.system,
            "release": uname.release,
            "version": uname.version,
            "machine": uname.machine,
            "processor": uname.processor,
        }

    @staticmethod
    def get_git_status() -> Dict[str,Any]:
        try:
            repo = git.Repo(search_parent_directories=True)
        except git.exc.InvalidGitRepositoryError:
            return {"error": "no git repository found"}
        return {
            "branch": repo.active_branch.name,
            "commit": repo.head.object.hexsha,
            "dirty": repo.is_dirty(),
            "remotes": {
                    r.name: list(r.urls)
                for r in repo.remotes
            }
        }

    @staticmethod
    def get_context() -> Dict[str,str]:
        return {
            "user": os.environ.get("USER", "unknown"),
            "host": platform.uname().node,
        }

    def save(self, filename: str) -> None:

        # this is unsafe! we don't really know that the files
        # have been flushed (I'm looking at you GDAL!)
        outputhashed = []
        for filename in self.outputs:
            hash = hashlib.sha1()
            with self.builtin_open(filename, "rb") as f:
                while chunk := f.read(1024 * hash.block_size):
                    hash.update(chunk)
            outputhashed.append({'path': filename, 'sha': hash.hexdigest()})


        document = {
            "start": self.start.isoformat(),
            "end": datetime.now(timezone.utc).isoformat(),
            "env": self.get_context(),
            "inputs": self.inputs,
            "outputs": outputhashed,
            "git": self.get_git_status(),
            "uname": self.get_platform(),
            "python": self.get_python_env(),
        }
        print("Shark manifest:")
        print(json.dumps(document, indent=4))

manifest = Manifest()
