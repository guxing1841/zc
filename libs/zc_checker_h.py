"""\
Checker common
Copyright (C) Zhou Changrong
"""
import os, sys
ZC_CHECKER_MODULE = 100
ZC_CHECKER_MAIN_CONF      =  0x02000000
ZC_CHECKER_GROUP_CONF     =  0x04000000
ZC_CHECKER_HOST_CONF      =  0x08000000
ZC_CHECKER_SVC_CONF       =  0x10000000

ZC_SHUTDOWN = 0x00000001
ZC_RESTART  = 0x00000002
ZC_READY    = 0x00000004
ZC_CHECKER_HOST_TASK  = 1
ZC_CHECKER_SVC_TASK  = 2


class zc_checker_module_ctx:
	def __init__(self, **args):
		self.service_types = None
		self.service_record = None
		self.host_record = None
		self.service_task = None
		self.merge_main_conf = None
		self.merge_group_conf = None
		self.merge_host_conf = None
		self.merge_service_conf = None
		self.host_attach_handlers = None
		self.service_attach_handlers = None
		for key in args.keys():
			if key == 'service_types':
				self.service_types = args[key]
			elif key == 'service_record':
				self.service_record = args[key]
			elif key == 'host_record':
				self.host_record = args[key]
			elif key == 'service_task':
				self.service_task = args[key]
			elif key == 'merge_main_conf':
				self.merge_main_conf = args[key]
			elif key == 'merge_group_conf':
				self.merge_group_conf = args[key]
			elif key == 'merge_host_conf':
				self.merge_host_conf = args[key]
			elif key == 'merge_service_conf':
				self.merge_service_conf = args[key]
			elif key == 'host_attach_handlers':
				self.host_attach_handlers = args[key]
			elif key == 'service_attach_handlers':
				self.service_attach_handlers = args[key]
			else:
				raise TypeError, "zc_checker_module_ctx() got an unexpected keyword argument '%s'" %(key)
				os._exit(1)



