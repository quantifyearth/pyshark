import hashlib
import json
import os
import platform
import sys
import uuid
from datetime import datetime, timezone
from multiprocessing import parent_process, shared_memory
from typing import Any,Dict

import git
import locket
import pkg_resources
import xattr


class Manifest:
    def __init__(self):
        self.start = datetime.now(timezone.utc)
        self.inputs = []
        self.outputs = set()

        # testing how to sync back and forth with multiprocessing
        shared_name = os.environ.get("SHARK_SHARED", None)
        if shared_name is None:
            self.lock_name = os.path.join("/tmp", "shark.lock")
            self.manifest_lists = shared_memory.ShareableList([self.lock_name, "[]"] + ([None,] * 500))
            self.manifest_list_index = 1
            os.environ["SHARK_SHARED"] = self.manifest_lists.shm.name
        else:
            self.manifest_lists = shared_memory.ShareableList(name=shared_name)
            self.lock_name = self.manifest_lists[0]
            with locket.lock_file(self.lock_name):
                for index in range(2, 500):
                    if self.manifest_lists[index] is None:
                        self.manifest_list_index = index
                        self.manifest_lists[index] = "[]"
                raw_parent_state = self.manifest_lists[1]
            self.nputs = json.loads(raw_parent_state)

        print(self.lock_name)
        # Use this moment to get any info before we shim things
        self.git = Manifest.get_git_status()

    def append_input(self, filename: str) -> None:
        if filename in [x['path'] for x in self.inputs]:
            return
        if filename == self.lock_name:
            return
        hash = hashlib.sha1()
        with self.builtin_open(filename, "rb") as f:
            while chunk := f.read(1024 * hash.block_size):
                hash.update(chunk)

        info = {'path': filename, 'sha': hash.hexdigest()}
        xattr_info = xattr.xattr(filename)
        if 'user.shark' in xattr_info:
            info['history'] = xattr_info['user.shark']

        self.inputs.append(info)
        with locket.lock_file(self.lock_name):
            self.manifest_lists[self.manifest_list_index] = json.dumps(self.inputs)

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

    def generate(self) -> Dict:
        # this is unsafe! we don't really know that the files
        # have been flushed (I'm looking at you GDAL!)
        outputhashed = []
        for filename in self.outputs:
            hash = hashlib.sha1()
            with self.builtin_open(filename, "rb") as f:
                while chunk := f.read(1024 * hash.block_size):
                    hash.update(chunk)
            outputhashed.append({'path': filename, 'sha': hash.hexdigest()})

        with locket.lock_file(self.lock_name):
            for index in range(3, 500):
                child_inputs = self.manifest_lists[index]
                if child_inputs is None:
                    break
                self.inputs += json.loads(child_inputs)

        document = {
            "args": sys.argv[1:],
            "start": self.start.isoformat(),
            "end": datetime.now(timezone.utc).isoformat(),
            "env": self.get_context(),
            "inputs": self.inputs,
            "outputs": outputhashed,
            "git": self.git,
            "uname": self.get_platform(),
            "python": self.get_python_env(),
        }
        return document

    def save(self) -> None:
        if len(self.outputs) < 1:
            return;
        manifest = json.dumps(self.generate())
        for output in self.outputs:
            try:
                xattr_info = xattr.xattr(output)
            except FileNotFoundError:
                continue
            xattr_info.update({
                'user.shark': manifest.encode('utf-8')
            })

    def close(self) -> None:
        self.manifest_lists.shm.close()
        if parent_process() is None:
            self.manifest_lists.shm.unlink()


manifest = Manifest()
