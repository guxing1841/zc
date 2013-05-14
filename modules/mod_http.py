"""\
HTTP module
Copyright (C) Zhou Changrong
"""
import os, sys, time
from zc_http_h import *
from zc_config_h import *
from zc_core_h import *
from zc_checker_h import *

HTTP_DEF_CONNECT_TIMEOUT = 10
HTTP_DEF_TIMEOUT = 30
HTTP_DEF_METHOD = 'GET'
HTTP_DEF_METHOD = 'HTTP/1.1'
HTTP_DEF_VERSION = 'HTTP/1.1'
HTTP_VERSIONS = ['HTTP/1.0',  'HTTP/1.1']

def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'http_connect_timeout', 'http_timeout', 'http_method', 'http_version')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'http_connect_timeout', 'http_timeout', 'http_method', 'http_version')
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	stype = child['ctx']['type']
	zc_dict_set_no_has(ctx, pctx, 'http_connect_timeout', 'http_timeout', 'http_method', 'http_version')
	ctx['http_connect_timeout'] = zc_dict_get_gt(ctx, 'http_connect_timeout', 0, HTTP_DEF_CONNECT_TIMEOUT)
	ctx['http_timeout'] = zc_dict_get_gt(ctx, 'http_timeout', 0, HTTP_DEF_TIMEOUT)
	ctx['http_method'] = ctx.get('http_method', HTTP_DEF_METHOD)
	ctx['http_version'] = ctx.get('http_version', HTTP_DEF_VERSION)
	if stype in module.ctx.service_types:
		if 'http_url' not in ctx:
			cf.log.warn('http_url is not set in block "%s" will disabled "%s"in line %d in %s' %(child['service'], stype, child['start_line'], child['conf_file']))
			return ZC_IGNORE
	return ZC_OK


def task_handler(log, task):
	conf = task['conf']
	mctx = conf['modules'][module.ctx_index]
	stype = conf['ctx']['type']
	url = mctx['http_url']
	key_args = {
		'method' : mctx['http_method'],
		'proxy_host' : mctx.get('http_proxy_host'),
		'proxy_port' : mctx.get('http_port'),
		'headers' : mctx.get('http_headers'),
		'connect_timeout' : int(mctx['http_connect_timeout']),
		'timeout' : int(mctx['http_timeout']),
		'userpwd' : mctx.get('userpwd')
		}
	c = httprequest(**key_args)
	now = time.time()
	stype = conf['ctx']['type']
	try:
		try:
			status = c.request(url)
		finally:
			c.close()
	except ZC_Error as e:
		use_time = time.time() - now
		log.error('%s "%s %s %s" %.6fs %s' %(task['task_info'], mctx['http_method'], mctx['http_url'], mctx['http_version'], use_time, e))
		return None
	if stype == 'nginx':
		m = re.match(r'^Active connections: (\d+) \nserver accepts handled requests\n (\d+) (\d+) (\d+) \nReading: (\d+) Writing: (\d+) Waiting: (\d+) \n$', status['body'])
		if m == None:
			use_time = time.time() - now
			log.debug("nginx status page:\n%s", status['body'])
			log.error('%s "%s %s %s" %.6fs Nginx status page not match' %(task['task_info'], mctx['http_method'], mctx['http_url'], mctx['http_version'], use_time))
			return None
		else:
			status['nginx_active_connections'] = int(m.group(1))
			status['nginx_accepts'] = int(m.group(1))
			status['nginx_handled'] = int(m.group(2))
			status['nginx_requests'] = int(m.group(3))
			status['nginx_reading'] = int(m.group(4))
			status['nginx_writing'] = int(m.group(5))
			status['nginx_waiting'] = int(m.group(6))
	del status['body']
	del status['header']
	use_time = time.time() - now
	log.info('%s "%s %s %s" %.6fs' %(task['task_info'], mctx['http_method'], mctx['http_url'], mctx['http_version'], use_time))
	status['_check_use_time_'] = use_time
	return status

def conf_set_httpversion_slot(cf, cmd, conf):
	if zc_conf_check_slot(cf, cmd, conf) != ZC_OK:
		return ZC_ERROR
	if cf.args[1] not in HTTP_VERSIONS:
		cf.log.error("Unsupport http version '%s' in %s at line %d" %(cf.args[1], cf.cur['conf_file'], cf.cur['start_line']))
	conf[cmd.key] = cf.args[1]


commands = [
	zc_command(
		name = 'http_proxy_host',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'http_proxy_host',
		describe = 'HTTP proxy host'
	),
	zc_command(
		name = 'http_proxy_port',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_int_slot,
		key = 'http_proxy_port',
		describe = 'HTTP proxy port'
	),
	zc_command(
		name = 'http_url',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'http_url',
		describe = 'HTTP url'
	),
	zc_command(
		name = 'http_method',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'http_method',
		describe = 'HTTP method'
	),
	zc_command(
		name = 'http_userpwd',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'http_userpwd',
		describe = 'HTTP userpwd'
	),
	zc_command(
		name = 'http_version',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = conf_set_httpversion_slot,
		key = 'http_version',
		describe = 'HTTP version'
	),
	zc_command(
		name = 'http_headers',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_1MORE|ZC_CONF_MULTI,
		set = None,
		key = 'http_headers',
		describe = 'HTTP headers'
	),
	zc_command(
		name = 'http_connect_timeout',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		key = 'http_connect_timeout',
		describe = 'HTTP connect timeout'
	),
	zc_command(
		name = 'http_timeout',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		key = 'http_timeout',
		describe = 'HTTP connect timeout'
	),
]

http_ctx = zc_checker_module_ctx(
	service_types = ['http', 'nginx'],
	service_task = task_handler,
	merge_main_conf = merge_main_conf,
	merge_group_conf = merge_group_conf,
	merge_host_conf = merge_host_conf,
	merge_service_conf = merge_service_conf
	)
	

module = zc_module(
	name = 'http',
	version = '0.1.0',
	type = ZC_CHECKER_MODULE,
	commands = commands,
	ctx = http_ctx
	)
