"""\
TCP module
Copyright (C) Zhou Changrong
"""
import os, sys, time, socket, types
from zc_config_h import *
from zc_core_h import *
from zc_checker_h import *

TCP_DEF_CONNECT_TIMEOUT = 10

def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'tcp_connect_timeout')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'tcp_connect_timeout')
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	stype = child['ctx']['type']
	zc_dict_set_no_has(ctx, pctx, 'tcp_connect_timeout')
	ctx['tcp_connect_timeout'] = zc_dict_get_ge(ctx, 'tcp_connect_timeout', 0, TCP_DEF_CONNECT_TIMEOUT)
	if stype in module.ctx.service_types:
		if 'tcp_host' not in ctx:
			cf.log.warn('tcp_host is not set in block "%s" will disabled "%s"in line %d in %s' %(child['service'], stype, child['start_line'], child['conf_file']))
			return ZC_IGNORE
		if 'tcp_port' not in ctx:
			cf.log.warn('tcp_port is not set in block "%s" will disabled "%s"in line %d in %s' %(child['service'], stype, child['start_line'], child['conf_file']))
			return ZC_IGNORE

	return ZC_OK




def task_handler(log, task):
	conf = task['conf']
	mctx = conf['modules'][module.ctx_index]
	timeout = mctx['tcp_connect_timeout']
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	if timeout > 0:
		sock.settimeout(timeout)
	now = time.time()
	stype = conf['ctx']['type']
	try:
		try:
			sock.connect((mctx['tcp_host'], mctx['tcp_port']))
		finally:
			sock.close()
			use_time = time.time() - now
	except socket.error, e:
		log.error('%s "%s:%d" %.6fs %s' %(task['task_info'],  mctx['tcp_host'], mctx['tcp_port'], use_time, e))
		return None
	log.info('%s "%s:%d" %.6fs' %(task['task_info'], mctx['tcp_host'], mctx['tcp_port'], use_time))
	return [{'key' : 'conn_time', 'value': "%.6f" %(use_time)}]


commands = [
	zc_command(
		name = 'tcp_host',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'tcp_host',
		describe = 'TCP host'
	),
	zc_command(
		name = 'tcp_port',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_int_slot,
		key = 'tcp_port',
		describe = 'TCP port'
	),
	zc_command(
		name = 'tcp_connect_timeout',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		key = 'tcp_connect_timeout',
		describe = 'TCP connect timeout'
	),
]

tcp_ctx = zc_checker_module_ctx(
	service_types = ['tcp'],
	service_task = task_handler,
	merge_main_conf = merge_main_conf,
	merge_group_conf = merge_group_conf,
	merge_host_conf = merge_host_conf,
	merge_service_conf = merge_service_conf
	)
	

module = zc_module(
	name = 'tcp',
	version = '0.1.0',
	type = ZC_CHECKER_MODULE,
	commands = commands,
	ctx = tcp_ctx
	)
