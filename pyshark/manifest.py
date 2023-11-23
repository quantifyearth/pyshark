import json
import os
import platform
import pkg_resources
import sys
from datetime import datetime, timezone
from typing import Any,Dict

class Manifest:
	def __init__(self):
		self.start = datetime.now(timezone.utc)
		self.inputs = set()
		self.outputs = set()

	def append_input(self, filename: str) -> None:
		self.inputs.add(filename)

	def append_output(self, filename: str) -> None:
		self.outputs.add(filename)

	@staticmethod
	def get_python_env() -> Dict[str,Any]:
		return {
			"version": sys.version,
			"packages": {p.project_name:p.version for p in pkg_resources.working_set},
		}

	@staticmethod
	def get_platform() -> Dict[str,Any]:
		uname = platform.uname()
		return {
			"system": uname.system,
			"release": uname.release,
			"version": uname.version,
			"machine": uname.machine,
			"processor": uname.processor,
		}

	def save(self, filename: str) -> None:
		document = {
			"start": self.start.isoformat(),
			"end": datetime.now(timezone.utc).isoformat(),
			# "env": dict(os.environ),
			"inputs": list(self.inputs),
			"outputs": list(self.outputs),
			"git": {},
			"uname": self.get_platform(),
			"python": self.get_python_env(),
		}
		print("Shark manifest:")
		print(json.dumps(document, indent=4))

manifest = Manifest()
