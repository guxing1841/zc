"""\
ZABBIX module
Copyright (C) Zhou Changrong
"""
import os, sys, time, json, socket, struct
from zc_common import *
from zc_config_h import *
from zc_core_h import *
from zc_checker_h import *

ZBX_DEF_SERVER_PORT      = 10051
ZBX_DEF_CONNECT_TIMEOUT  = 10
ZBX_DEF_TIMEOUT          = 10
ZBX_VERSION 		 = 'ZBXD\1'
ZBX_VERSION_LEN          = 5
ZBX_DEF 		 = True
ZBX_DEF_BASEKEY	         = 'custom'

def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'zbx', 'zbx_basekey', 'zbx_servers', 'zbx_connect_timeout', 'zbx_timeout')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'zbx', 'zbx_basekey', 'zbx_servers', 'zbx_connect_timeout', 'zbx_timeout')
	ctx['zbx'] = ctx.get('zbx', ZBX_DEF)
	ctx['zbx_basekey'] = ctx.get('zbx_basekey', ZBX_DEF_BASEKEY)
	ctx['zbx_host'] = ctx.get('zbx_host', child['host'])
	ctx['zbx_connect_timeout'] = zc_dict_get_gt(ctx, 'zbx_connect_timeout', 0, ZBX_DEF_CONNECT_TIMEOUT)
	ctx['zbx_timeout'] = zc_dict_get_gt(ctx, 'zbx_timeout', 0, ZBX_DEF_TIMEOUT)
	if ctx['zbx'] and 'zbx_servers' not in ctx:
		cf.log.warn('command \'zbx_servers\' is not set in block "%s" in line %d in %s' %(child['service'], child['start_line'], child['conf_file']))
		return ZC_IGNORE
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'zbx', 'zbx_basekey', 'zbx_servers', 'zbx_connect_timeout', 'zbx_timeout', 'zbx_host')
	return ZC_OK

def zabbix_send_data(log, server, port, connect_timeout, timeout, data):
	zbx_data = json.dumps(data, indent=1)
	log.debug('zbx send data: %s' %(zbx_data))
	buf = ''
	try:
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.settimeout(connect_timeout)
			sock.connect((server, port))
			sock.settimeout(timeout)
			wbytes = 0
			sock_send(sock, ZBX_VERSION)
			length = len(zbx_data)
			len_str = struct.pack('Q', length)
			sock.send(len_str)
			sock_send(sock, zbx_data)
			version = sock_recv(sock, ZBX_VERSION_LEN)
			if len(version) != ZBX_VERSION_LEN:
				raise ZC_Error, "recv ZBX_VERSION_LEN miss match"
			len_str = sock_recv(sock, struct.calcsize('Q'))
			if len(len_str) != struct.calcsize('Q'):
				raise ZC_Error, "recv data len error"
			length = struct.unpack('Q', len_str)[0]
			buf = sock_recv(sock, length)
			if len(buf) != length:
				raise ZC_Error, 'recv unexpected eof' %(len(buf), length)
		finally:
			sock.close()
	except socket.error as e:
		raise ZC_Error, '%s' %(e)
	log.debug("zbx recv data: %s" %(buf))
	return ZC_OK

def host_send_handler(log, task):
	conf = task['conf']
	hctx = conf['ctx']
	mctx = conf['modules'][module.ctx_index]
	if not mctx['zbx']:
		return ZC_IGNORE
	host = mctx['zbx_host']
	key = mctx['zbx_basekey']
	connect_timeout = mctx['zbx_connect_timeout']
	timeout = mctx['zbx_timeout']
	data = {}
	for sconf in conf['services']:
		smctx = sconf['modules'][module.ctx_index]
		sctx = sconf['ctx']
		servicename = sconf['service']
		if 'disable' in sctx and sctx['disable']:
			continue
		stype = sctx['type']
		if stype in data:
			data[stype].append({'{#SERVER}' : servicename})
		else:
			data[stype] = [{'{#SERVER}' : servicename}]
	zbx_data = {'request': 'sender data', 'data' : []}
	for stype in data.keys():
		zbx_data['data'].append({'key' : "%s.%s" %(key, stype), 'host' : host, 'value' : json.dumps({'data' : data[stype]})})
	for s in mctx['zbx_servers']:
		use_time = 0
		sp = s.split(':')
		server = sp[0]
		port = 0
		if len(sp) > 1:
			try:
				port = int(sp[1])
			except ValueError, e:
				log.error("Port '%s' is not integer" %(sp[1]))
				continue
		if port <= 0:
			port = ZBX_DEF_SERVER_PORT
		now = time.time()
		try:
			try:
				zabbix_send_data(log, server, port, connect_timeout, timeout, zbx_data)
			finally:
				use_time = time.time() - now
		except ZC_Error as e:
			log.error('zbx_host %s %s "%s:%d" %.6fs %s' %(task['task_info'], stype, server, port, use_time, e))
			continue
		log.info('zbx_host %s %s "%s:%d" %.6fs' %(task['task_info'], stype, server, port, use_time))
		continue
	return ZC_OK

def service_send_handler(log, task, data):
	conf = task['conf']
	sctx = conf['ctx']
	mctx = conf['modules'][module.ctx_index]
	if not mctx['zbx']:
		return ZC_IGNORE
	host = mctx['zbx_host']
	stype = sctx['type']
	key = mctx['zbx_basekey']
	connect_timeout = mctx['zbx_connect_timeout']
	timeout = mctx['zbx_timeout']
	use_time = 0
	for s in mctx['zbx_servers']:
		sp = s.split(':')
		server = sp[0]
		port = 0
		if len(sp) > 1:
			try:
				port = int(sp[1])
			except ValueError, e:
				log.error("Port '%s' is not integer" %(sp[1]))
				continue
		if port <= 0:
			port = ZBX_DEF_SERVER_PORT
		zbx_data = {'request': 'sender data', 'data' : []}
		for rkey in data.keys():
			zbx_data['data'].append({'key' : "%s.%s.%s[%s]" %(key, stype, rkey, conf['service']), 'host' : host, 'value' : data[rkey]})
		now = time.time()
		try:
			try:
				zabbix_send_data(log, server, port, connect_timeout, timeout, zbx_data)
			finally:
				use_time = time.time() - now
		except ZC_Error as e:
			log.error('zbx_svc %s "%s:%d" %.6fs %s' %(task['task_info'], server, port, use_time, e))
			continue
		log.info('zbx_svc %s "%s:%d" %.6fs' %(task['task_info'], server, port, use_time))
		continue
	return ZC_OK

commands = [
	zc_command(
	name = 'zbx',
	type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_flag_slot,
	describe = 'Zabbix flag',
	key = 'zbx',
	),

	zc_command(
	name = 'zbx_servers',
	type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CONF_1MORE|ZC_CONF_MULTI,
	set = None,
	describe = 'Zabbix active server to sent',
	key = 'zbx_servers',
	),
	zc_command(
	name = 'zbx_basekey',
	type =  ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_str_slot,
	describe = 'Zabbix base key',
	key = 'zbx_basekey',
	),
	zc_command(
	name = 'zbx_host',
	type = ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_int_slot,
	describe = 'Zabbix host',
	key = 'zbx_host',
	),
	zc_command(
	name = 'zbx_connect_timeout',
	type = ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_sec_slot,
	describe = 'Zabbix connect timeout',
	key = 'zbx_connect_timeout',
	),
	zc_command(
	name = 'zbx_timeout',
	type = ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_int_slot,
	describe = 'Zabbix timeout',
	key = 'zbx_timeout',
	),
]

zabbix_ctx = zc_checker_module_ctx(
	service_types = [],
	host_record = host_send_handler,
	service_record = service_send_handler,
	merge_main_conf = merge_main_conf,
	merge_group_conf = merge_group_conf,
	merge_host_conf = merge_host_conf,
	merge_service_conf = merge_service_conf,
	)


module = zc_module(
	name = 'zabbix',
	version = '0.1.0',
	type = ZC_CHECKER_MODULE,
	commands = commands,
	ctx = zabbix_ctx
	)


