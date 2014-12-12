"""\
Checker common
Copyright (C) Zhou Changrong
"""
import os, sys
ZC_SIMPLE_MODULE = 101
ZC_SIMPLE_MAIN_CONF      =  0x20000000
ZC_SIMPLE_TASK_CONF     =  0x40000000

class zc_simple_module_ctx:
	def __init__(self, **args):
		self.task_types = None
		self.merge_main_conf = None
		self.merge_task_conf = None
		self.process_handler = None
		for key in args.keys():
			if key == 'task_types':
				self.task_types = args[key]
			elif key == 'merge_main_conf':
				self.merge_main_conf = args[key]
			elif key == 'merge_task_conf':
				self.merge_task_conf = args[key]
			elif key == 'process_handler':
				self.process_handler = args[key]
			else:
				raise TypeError, "zc_simple_module_ctx() got an unexpected keyword argument '%s'" %(key)
				os._exit(1)



