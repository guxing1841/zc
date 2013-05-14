"""\
Redis module
Copyright (C) Zhou Changrong
"""
import os, sys, time, redis, types
from zc_config import *
from zc_core_h import *
from zc_checker_h import *

REDIS_DEF_PORT = 6379
REDIS_DEF_SOCKET_TIMEOUT = 3

def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'redis_port', 'redis_socket_timeout')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'redis_port', 'redis_socket_timeout')
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	stype = child['ctx']['type']
	zc_dict_set_no_has(ctx, pctx, 'redis_port', 'redis_socket_timeout')
	ctx['redis_port'] = zc_dict_get_gt(ctx, 'redis_port', 0, REDIS_DEF_PORT)
	ctx['redis_socket_timeout'] = zc_dict_get_gt(ctx, 'redis_socket_timeout', 0, REDIS_DEF_SOCKET_TIMEOUT)
	if stype in module.ctx.service_types:
		if 'redis_host' not in ctx:
			cf.log.warn('redis_host is not set in block "%s" will disabled "%s"in line %d in %s' %(child['service'], stype, child['start_line'], child['conf_file']))
			return ZC_IGNORE

	return ZC_OK


def task_handler(log, task):
	conf = task['conf']
	mctx = conf['modules'][module.ctx_index]
	now = time.time()
	status = {}
	stype = conf['ctx']['type']
	key_args = {'socket_timeout' : mctx['redis_socket_timeout']}
	args = {}
	if 'redis_passwd' in mctx:
		args['redis_passwd'] = mctx['redis_passwd']
	try:
		conn = redis.Redis(mctx['redis_host'], mctx['redis_port'], **key_args)
		info = conn.info()
	except  redis.exceptions.RedisError, e:	
		use_time = time.time() - now
		log.error('%s "%s:%d" %.6fs "%s"' %(task['task_info'], mctx['redis_host'], mctx['redis_port'], use_time, e.args[0]))
		return None
		
	status = {}
	for k in info.keys():
		if type(info[k]) == types.DictType:
			for k2 in info[k].keys():
				status["%s_%s" %(k,k2)] = info[k][k2]
		else:
			status[k] = info[k]
	use_time = time.time() - now
	if len(status) == 0:
		log.error('%s "%s:%d" %.6fs "can\'t get status"' %(task['task_info'], mctx['redis_host'], mctx['redis_port'], use_time))
		return None
	log.info('%s "%s:%d" %.6fs"' %(task['task_info'], mctx['redis_host'], mctx['redis_port'], use_time))
	status['_check_use_time_'] = use_time
	return status

commands = [
	zc_command(
		name = 'redis_host',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'redis_host',
		describe = 'Redis host'
	),
	zc_command(
		name = 'redis_port',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_int_slot,
		key = 'redis_port',
		describe = 'Memcache or tterver port'
	),
	zc_command(
		name = 'redis_socket_timeout',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		key = 'redis_socket_timeout',
		describe = 'Redis socket timeout'
	),
	zc_command(
		name = 'redis_passwd',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'redis_passwd',
		describe = 'Redis password'
	),
]

redis_ctx = zc_checker_module_ctx(
	service_types = ['redis'],
	service_task = task_handler,
	merge_main_conf = merge_main_conf,
	merge_group_conf = merge_group_conf,
	merge_host_conf = merge_host_conf,
	merge_service_conf = merge_service_conf,
	)

module = zc_module(
	name = 'redis',
	version = '0.1.0',
	type = ZC_CHECKER_MODULE,
	commands = commands,
	ctx = redis_ctx
	)
