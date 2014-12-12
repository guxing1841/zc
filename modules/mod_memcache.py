"""\
Memcache and ttserver module
Copyright (C) Zhou Changrong
"""
import os, sys, time, memcache
from zc_config import *
from zc_core_h import *
from zc_checker_h import *
MC_DEF_PORT = 11211
TS_DEF_PORT = 1978

MC_DEF_SOCKET_TIMEOUT = 3

def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'mc_port', 'mc_socket_timeout')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'mc_port', 'mc_socket_timeout')
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	stype = child['ctx']['type']
	zc_dict_set_no_has(ctx, pctx, 'mc_port', 'mc_socket_timeout')
	ctx['mc_socket_timeout'] = zc_dict_get_ge(ctx, 'mc_socket_timeout', 0, MC_DEF_SOCKET_TIMEOUT)
	if stype == 'memcache':
		ctx['mc_port'] = zc_dict_get_gt(ctx, 'mc_port', 0, MC_DEF_PORT)
	elif stype == 'ttserver':
		ctx['mc_port'] = zc_dict_get_gt(ctx, 'mc_port', 0, TS_DEF_PORT)
	return ZC_OK


def task_handler(log, task):
	conf = task['conf']
	mctx = conf['modules'][module.ctx_index]
	stype = conf['ctx']['type']
	now = time.time()
	status = {}
	stype = conf['ctx']['type']
	key_args = {'debug' : 0}
	timeout = int(mctx['mc_socket_timeout'])
	if memcache.__version__ >= '1.48' and timeout > 0:
		key_args['socket_timeout'] = timeout
	conn = memcache.Client(["%s:%d" %(mctx['mc_host'], mctx['mc_port'])], **key_args)
	status = conn.get_stats()
	conn.disconnect_all()
	if len(status) == 0:
		use_time = time.time() - now
		log.error('%s "%s:%d" %.6fs Can\'t get status' %(task['task_info'], mctx['mc_host'], mctx['mc_port'], use_time))
		return None
	status = status[0][1]
	use_time = time.time() - now
	log.info('%s "%s:%d" %.6fs"' %(task['task_info'], mctx['mc_host'], mctx['mc_port'], use_time))
	r = []
	for key in status:
		r.append({'key' : key, 'value': status[key]})
	return r

commands = [
	zc_command(
		name = 'mc_host',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'mc_host',
		describe = 'Memcache or ttserver host'
	),
	zc_command(
		name = 'mc_port',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_int_slot,
		key = 'mc_port',
		describe = 'Memcache or tterver port'
	),
	zc_command(
		name = 'mc_socket_timeout',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		key = 'mc_socket_timeout',
		describe = 'Memcache or ttserver socket timeout'
	)
]

mc_ctx = zc_checker_module_ctx(
	service_types = ['memcache', 'ttserver'],
	service_task = task_handler,
	merge_main_conf = merge_main_conf,
	merge_group_conf = merge_group_conf,
	merge_host_conf = merge_host_conf,
	merge_service_conf = merge_service_conf,
	)

module = zc_module(
	name = 'memcache',
	version = '0.1.0',
	type = ZC_CHECKER_MODULE,
	commands = commands,
	ctx = mc_ctx
	)
