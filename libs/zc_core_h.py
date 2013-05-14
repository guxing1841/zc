"""\
Core common
Copyright (C) Zhou Changrong
"""

import os, sys

ZC_CORE_MODULE = 1
class zc_core_module_ctx():
	def __init__(self, **args):
		self.init_master = None
		self.init_process = None
		for key in args.keys():
			if key == 'init_master':
				self.init_master = args[key]
			elif key == 'init_process':
				self.init_process = args[key]
			else:
				raise TypeError, "zc_core_module_ctx() got an unexpected keyword argument '%s'" %(key)
				os._exit(1)


class zc_module():
	def __init__(self, **args):
		self.ctx_index = -1
		self.name = None
		self.version = None
		self.type = -1
		self.commands = None
		self.init_master = None
		self.process_task = None
		self.ctx = None
		for key in args.keys():
			if key == 'name':
				self.name = args[key]
			elif key == 'version':
				self.version = args[key]
			elif key == 'type':
				self.type = args[key]
			elif key == 'commands':
				self.commands = args[key]
			elif key == 'init_master':
				self.init_master = args[key]
			elif key == 'init_process':
				self.init_process = args[key]
			elif key == 'process_task':
				self.process_task = args[key]
			elif key == 'ctx':
				self.ctx = args[key]
			else:
				raise TypeError, "zc_module() got an unexpected keyword argument '%s'" %(key)
				os._exit(1)

class zc_command():
	def __init__(self, **args):
		self.ctx_index = -1
		self.name = None
		self.type = 0
		self.set = None
		self.describe = None
		self.key = None
		for key in args.keys():
			if key == 'name':
				self.name = args[key]
			elif key == 'type':
				self.type = args[key]
			elif key == 'set':
				self.set = args[key]
			elif key == 'describe':
				self.describe = args[key]
			elif key == 'key':
				self.key = args[key]
			else:
				raise TypeError, "zc_command() got an unexpected keyword argument '%s'" %(key)
				os._exit(1)

def is_running(pids):
	for pid in pids:
		try:
			os.kill(pid, 0)
			return True
		except:
			continue
	return False

def kill_pids(pids, sig):
	for pid in pids:
		try:
			os.kill(pid, sig)
		except:
			continue

class pid_file():
	def __init__(self, file):
		self.file = file
	def write(self, pids):
		fd = os.open(self.file, os.O_CREAT|os.O_TRUNC|os.O_WRONLY)
		os.write(fd, " ".join(str(pid) for pid in pids))
		os.close(fd)
	def is_exists(self):
		return os.path.exists(self.file)
	def read(self):
		fh = open(self.file, 'r')
		line = fh.readline()
		line = line.rstrip('\n')
		if line == "":
			pids = []
		else:
			pids = [int(pid) for pid in line.split(" ")]
		fh.close()
		return pids

	def remove(self):
		try:
			os.unlink(self.file)
			return True
		except:
			return False

