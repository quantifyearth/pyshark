import copy
import hashlib
import json
import os
import platform
import struct
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from multiprocessing import parent_process, shared_memory
from typing import Any, Dict, Optional

import git
import locket
import pkg_resources
import xattr

@dataclass
class FileRef:
    path: str
    sha: str
    history: Optional[str] = None

    def __hash__(self) -> int:
        return (self.path + self.sha).__hash__()

    def to_dict(self):
        res = {"path": self.path, "sha": self.sha}
        if self.history is not None:
            res["history"] = self.history
        return res

    @classmethod
    def from_dict(cls, info: Dict[str,str]) -> "FileRef":
        return FileRef(info["path"], info["sha"], info.get("history", None))


class Manifest:

    def __init__(self):
        # on linux this is called once in the parent process and then copied
        # over via fork/exec
        self.start = datetime.now(timezone.utc)
        self.inputs = set()
        self.outputs = set()
        self.lock_name = os.path.join("/tmp", "shark.lock")
        self.fd_cache = {}

        if parent_process() is None:
            self.child_input_lists = shared_memory.SharedMemory(create=True, size=1024 * 1024 * 128)
            self.child_input_lists.buf[:8] = struct.pack("@Q", 0)

        # Use this moment to get any info before we shim things
        self.git = Manifest.get_git_status()

        self.stack = []

    @staticmethod
    def side_file_name(original_filename: str) -> str:
        path, filename = os.path.split(original_filename)
        return os.path.join(path, f".{filename}.shark")

    def append_input(self, filename: str, descriptor: Optional[int]=None) -> None:
        if not isinstance(filename, str):
            return None
        if filename == self.lock_name:
            return None
        fqname = os.path.abspath(os.path.join(os.getcwd(), filename))
        if fqname in [x.path for x in self.inputs]:
            return fqname
        hash = hashlib.sha1()
        with self.builtin_open(fqname, "rb") as f:
            try:
                while chunk := f.read(1024 * hash.block_size):
                    hash.update(chunk)
                hashdigest = hash.hexdigest()
            except OSError:
                print(f"Failed to hash {fqname}", file=sys.stderr)
                hashdigest = "unknown"

        info = FileRef(fqname, hashdigest)
        xattr_info = xattr.xattr(fqname)
        if 'user.shark' in xattr_info:
            info.history = str(xattr_info['user.shark'])
        else:
            sidefilename = Manifest.side_file_name(fqname)
            try:
                with self.builtin_open(sidefilename, "r") as sidefile:
                    info.history = sidefile.read()
            except OSError:
                pass

        self.inputs.add(info)
        if descriptor is not None:
            self.fd_cache[descriptor] = info

    def append_output(self, filename: str, descriptor: Optional[int]=None) -> None:
        if not isinstance(filename, str):
            return
        if filename == self.lock_name:
            return        
        fqname = os.path.abspath(os.path.join(os.getcwd(), filename))
        self.outputs.add(fqname)

        if descriptor is not None:
            self.fd_cache[descriptor] = fqname

    def close_fd(self, descriptor: int) -> None:
        try:
            ref = self.fd_cache[descriptor]
        except KeyError:
            return
        if isinstance(ref, str):
            if ref in self.outputs:
                manifest = json.dumps(self.generate())
                self._save_to_file(ref, manifest)
                self.outputs.remove(ref)
        del self.fd_cache[descriptor]

    def snapshot(self):
        self.stack.append((copy.copy(self.inputs), copy.copy(self.outputs)))

    def restore(self):
        self.inputs, self.outputs = self.stack.pop()

    def child_flush(self):
        # in theory this could be compbined with save if we knew we were a child
        with locket.lock_file(self.lock_name):
            buffer = self.child_input_lists.buf
            length = struct.unpack('@Q', buffer[:8])[0]
            if length > 0:
                raw = bytes(buffer[8:length + 8])
                currentlist = json.loads(raw.decode("utf-8"))
            else:
                currentlist = []
            current = set([FileRef.from_dict(x) for x in currentlist])

            current = current.union(self.inputs)
            raw = json.dumps([x.to_dict() for x in current]).encode("utf-8")
            buffer[0:8] = struct.pack("@Q", len(raw))
            buffer[8:len(raw)+8] = raw

    def parent_flush(self):
        if parent_process() is None:
            with locket.lock_file(self.lock_name):
                buffer = self.child_input_lists.buf
                length = struct.unpack('@Q', buffer[:8])[0]
                if length > 0:
                    raw = bytes(buffer[8:length + 8])
                    currentlist = json.loads(raw.decode("utf-8"))
                    current = set([FileRef.from_dict(x) for x in currentlist])
                    self.inputs = self.inputs.union(current)

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

        document = {
            "args": sys.argv[1:],
            "start": self.start.isoformat(),
            "end": datetime.now(timezone.utc).isoformat(),
            "env": self.get_context(),
            "inputs": [x.to_dict() for x in self.inputs],
            "outputs": outputhashed,
            "git": self.git,
            "uname": self.get_platform(),
            "python": self.get_python_env(),
        }
        return document

    def _save_to_file(self, filename: str, manifest: str) -> None:
        try:
            xattr_info = xattr.xattr(filename)
        except FileNotFoundError:
            return
        try:
            xattr_info.update({
                'user.shark': manifest.encode('utf-8')
            })
        except OSError:
            # if we can't write data as xattr, drop it as a side file
            sidefilename = Manifest.side_file_name(filename)
            try:
                with self.builtin_open(sidefilename, "w") as sidefile:
                    sidefile.write(manifest)
            except OSError:
                print(f"Failed to write manifest for {filename}", file=sys.stderr)

    def save(self) -> None:
        manifest = json.dumps(self.generate())
        for output in self.outputs:
            self._save_to_file(output, manifest)

    def close(self) -> None:
        if self.child_input_lists is not None:
            shared_list = self.child_input_lists
            self.child_input_lists = None
            if parent_process() is None:
                try:
                    shared_list.unlink()
                except FileNotFoundError:
                    pass


manifest = Manifest()
