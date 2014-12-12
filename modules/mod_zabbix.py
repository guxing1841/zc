"""\
ZABBIX module
Copyright (C) Zhou Changrong
"""
import os, sys, time, socket, struct, errno, select
import tempfile
try:
	import json
except ImportError, e:
	import simplejson as json

from zc_common import *
from zc_config_h import *
from zc_core_h import *
from zc_checker_h import *
import zc_lock

ZBX_DEF_SERVER_PORT      = 10051
ZBX_DEF_CONNECT_TIMEOUT  = 10
ZBX_DEF_TIMEOUT          = 30
#ZBX_VERSION 		 = 'ZBXD\1'
ZBX_VERSION 		 = 'ZBXC\1'
ZBX_VERSION_LEN          = 5
ZBX_OK			 = 'OK'
ZBX_OK_LEN		 = 2
ZBX_DEF 		 = True
#ZBX_DEF_ACTIVE          = True
ZBX_DEF_BASEKEY          = 'zc'
ZBX_DEF_SUFFIX           = True
ZBX_DEF_UNSEND_LOCKFILE  = 'logs/zc_zbx_unsend.lock'
ZBX_DEF_RESEND_INTERVAL  = 5
unsend_list_lock         = None
unsend_list_file         = 'logs/unsend/.unsend_list'
ZBX_DEF_UNSEND_DIR       = 'logs/unsend'

def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	if 'zbx_resend_interval' not in ctx:
		ctx['zbx_resend_interval'] = ZBX_DEF_RESEND_INTERVAL
	if 'zbx_unsend_lockfile' not in ctx:
		ctx['zbx_unsend_lockfile'] = ZBX_DEF_UNSEND_LOCKFILE
	if 'zbx_unsend_dir' not in ctx:
		ctx['zbx_unsend_dir'] = ZBX_DEF_UNSEND_DIR
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
#	zc_dict_set_no_has(ctx, pctx, 'zbx', 'zbx_active', 'zbx_active_interval', 'zbx_basekey', 'zbx_servers', 'zbx_connect_timeout', 'zbx_timeout', 'zbx_suffix')
	zc_dict_set_no_has(ctx, pctx, 'zbx', 'zbx_basekey', 'zbx_servers', 'zbx_connect_timeout', 'zbx_timeout', 'zbx_suffix')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
#	zc_dict_set_no_has(ctx, pctx, 'zbx', 'zbx_active', 'zbx_active_interval', 'zbx_basekey', 'zbx_servers', 'zbx_connect_timeout', 'zbx_timeout')
	zc_dict_set_no_has(ctx, pctx, 'zbx', 'zbx_basekey', 'zbx_servers', 'zbx_connect_timeout', 'zbx_timeout')
	ctx['zbx'] = ctx.get('zbx', ZBX_DEF)
#	ctx['zbx_active'] = ctx.get('zbx_active', ZBX_DEF_ACTIVE)
#	ctx['zbx_active_interval'] = zc_dict_get_gt(ctx, 'zbx_active_interval', 0, child['ctx']['host_interval'])
	ctx['zbx_basekey'] = ctx.get('zbx_basekey', ZBX_DEF_BASEKEY)
	ctx['zbx_host'] = ctx.get('zbx_host', child['host'])
	ctx['zbx_connect_timeout'] = zc_dict_get_ge(ctx, 'zbx_connect_timeout', 0, ZBX_DEF_CONNECT_TIMEOUT)
	ctx['zbx_timeout'] = zc_dict_get_ge(ctx, 'zbx_timeout', 0, ZBX_DEF_TIMEOUT)
	ctx['zbx_suffix'] = ctx.get('zbx_suffix', ZBX_DEF_SUFFIX)
	if ctx['zbx'] and 'zbx_servers' not in ctx:
		cf.log.warn('command \'zbx_servers\' is not set in block "%s" in line %d in %s' %(child['service'], child['start_line'], child['conf_file']))
		return ZC_IGNORE
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'zbx', 'zbx_basekey', 'zbx_servers', 'zbx_connect_timeout', 'zbx_timeout', 'zbx_host', 'zbx_suffix')
	return ZC_OK

def unsend_list_add(listfile, items):
	f = open(listfile, "a") 
	for item in items:
		f.write("%s\n" %item)
	f.close()
	
def unsend_list_del(listfile, items):
	f = open(listfile, "r")
	lines = f.readlines()
	count = 0
	for item in items:
		try:
			lines.remove(item + "\n")
			count += 1
		except ValueError, e:
			continue
	f.close()
	if count > 0:
		f = open(listfile, "w")
		count = 0
		for line in lines:
			f.write(line)
			count += len(line)
		f.truncate(count)
		f.close()

def save_tempfile(data):
	global unsend_dir
	suffix = str(time.time())
	suffix = suffix.replace('.', ',')
	f = tempfile.NamedTemporaryFile(prefix='unsend', suffix=suffix, delete=False, dir=unsend_dir)
	filename = f.name
	f.write(data)
	f.close()
	return filename

def unsend_list_getall(listfile):
	f = open(listfile, 'r')
	unsend_list = []
	while True:
		line = f.readline()
		if line == "":
			break
		unsend_list.append(line.rstrip("\n"))
	f.close()
	return unsend_list

def read_tempfile(file):
	f = open(file, "r")
	tmp = ''
	while True:
		buf = f.read(1024)
		if buf == "":
			break
		tmp += buf
	data = json.loads(tmp)
	return data
		
		
	

def zbx_send_data(log, server, port, connect_timeout, timeout, data):
	buf = ''
	while True:
		try:
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				if connect_timeout > 0:
					sock.settimeout(connect_timeout)
				else:
					sock.settimeout(None)
				sock.connect((server, port))
				if timeout > 0:
					sock.settimeout(timeout)
				else:
					sock.settimeout(None)
				wbytes = 0
				while True:
					try:
						sock_send(sock, ZBX_VERSION)
					except socket.error, e:
						if e[0] == errno.EINTR:
							continue
						raise socket.error, e
					break
				ok = sock_recv(sock, ZBX_OK_LEN)
				if len(ok) != ZBX_OK_LEN or ok != ZBX_OK:
					raise ZC_ERROR, "recv server ok check miss match"
				t = time.time()
				clock = int(t)
				ns = int((t-clock)*1000000000)
				data['clock'] = clock
				data['ns'] = ns
				zbx_data = json.dumps(data, indent=1)
				log.debug('zbx send data: %s' %(zbx_data))
				length = len(zbx_data)
				len_str = struct.pack('Q', length)
				while True:
					try:
						sock.send(len_str)
					except socket.error, e:
						if e[0] == errno.EINTR:
							continue
						raise socket.error, e
					break
				while True:
					try:
						sock_send(sock, zbx_data)
					except socket.error, e:
						if e[0] == errno.EINTR:
							continue
						raise socket.error, e
					break
				version = sock_recv(sock, ZBX_VERSION_LEN)
				if len(version) != ZBX_VERSION_LEN:
					raise ZC_Error, "recv ZBX_VERSION_LEN miss match"
				while True:
					try:
						len_str = sock_recv(sock, struct.calcsize('Q'))
					except socket.error, e:
						if e[0] == errno.EINTR:
							continue
						raise socket.error, e
					break
				if len(len_str) != struct.calcsize('Q'):
					raise ZC_Error, "recv data len error"
				length = struct.unpack('Q', len_str)[0]
				while True:
					try:
						buf = sock_recv(sock, length)
					except socket.error, e:
						if e[0] == errno.EINTR:
							continue
						raise socket.error, e
					break
				if len(buf) != length:
					raise ZC_Error, 'recv unexpected eof(%d, %d)' %(len(buf), length)
			finally:
				sock.close()
		except socket.error, e:
			if e[0] == errno.EINTR:
				continue
			raise ZC_Error, '%s' %(e)
		break
	log.debug("ZBX recv data: %s" %(buf))
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
		
		if 'zbx_suffix' in smctx and not smctx['zbx_suffix']:
			continue
		stype = sctx.get('custom_type', sctx['type'])
		if stype in data:
			data[stype].append({'{#SERVER}' : servicename})
		else:
			data[stype] = [{'{#SERVER}' : servicename}]
	t = time.time()
	clock = int(t)
	ns = int((t-clock)*1000000000)
	zbx_data = {'request': 'sender data', 'data' : []}
	for stype in data:
		tmp = data[stype]
		zbx_data['data'].append({'key' : "%s.%s" %(key, stype), 'host' : host, 'value' : json.dumps({'data' : tmp})})
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
		now = time.time()
		try:
			try:
				zbx_send_data(log, server, port, connect_timeout, timeout, zbx_data)
			finally:
				use_time = time.time() - now
		except ZC_Error, e:
			log.error('zbx_host %s %s "%s" %.6fs %s' %(task['task_info'], server, port, use_time, e))
			continue
		log.info('zbx_host %s %s "%s" %.6fs' %(task['task_info'], server, port, use_time))
		continue
	return ZC_OK

def service_send_handler(log, task, data):
	conf = task['conf']
	sctx = conf['ctx']
	mctx = conf['modules'][module.ctx_index]
	if not mctx['zbx']:
		return ZC_IGNORE
	host = mctx['zbx_host']
	stype = sctx.get('custom_type', sctx['type'])
	key = mctx['zbx_basekey']
	suffix = mctx['zbx_suffix']
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
		t = time.time()
		clock = int(t)
		ns = int((t-clock)*1000000000)
		zbx_data = {'request': 'sender data', 'data' : []}
		for item in data:
			tmp = {}
			tmp['clock'] = clock
			tmp['ns'] = ns
			tmp['host'] = host
			if 'clock' in item and item['clock'] > 0:
				tmp['clock'] = item['clock']
				tmp['ns'] = 0
				if 'ns' in item and item['ns'] > 0:
					tmp['ns'] = item['ns']
			if isinstance(item['value'], (list, dict)):
				tmp['value'] = json.dumps({'data': item['value']})
			elif not isinstance(item['value'], str):
				tmp['value'] = str(item['value'])
			else:
				tmp['value'] = item['value']

			tmp['key'] = "%s.%s.%s" %(key, stype, item['key'])
			if suffix:
				tmp['key'] = "%s[%s]" %(tmp['key'], conf['service'])

			zbx_data['data'].append(tmp)
		now = time.time()
		try:
			try:
				zbx_send_data(log, server, port, connect_timeout, timeout, zbx_data)
			finally:
				use_time = time.time() - now
		except ZC_Error, e:
			log.error('%s "%s:%d" %.6fs %s' %(task['task_info'], server, port, use_time, e))
			global unsend_list_file
			global unsend_list_lock
			try:
				tempfile = save_tempfile(
					json.dumps(
						{
							'server' : server,
							'port' : port,
							'connect_timeout' : connect_timeout,
							'timeout' : timeout,
							'data' : zbx_data
						}))
				unsend_list_lock.lock(unsend_list_lock.LOCK_EX)
				unsend_list_add(unsend_list_file, [tempfile])
				unsend_list_lock.unlock()
			except Exception, e:
				log.error('Save tempfile %s "%s:%d" %s' %(task['task_info'], server, port, e))
			continue
		log.info('%s "%s:%d" %.6fs' %(task['task_info'], server, port, use_time))
		continue
	return ZC_OK

#def active_send_handler(log, task):
#	conf = task['conf']
#	hctx = conf['ctx']
#	mctx = conf['modules'][module.ctx_index]
#	if not mctx['zbx']:
#		return ZC_IGNORE
#	host = mctx['zbx_host']
#	key = mctx['zbx_basekey']
#	connect_timeout = mctx['zbx_connect_timeout']
#	timeout = mctx['zbx_timeout']
#	data = {}
#	for sconf in conf['services']:
#		smctx = sconf['modules'][module.ctx_index]
#		sctx = sconf['ctx']
#		servicename = sconf['service']
#		if 'disable' in sctx and sctx['disable']:
#			continue
#		stype = sctx['type']
#		if stype in data:
#			data[stype].append({'{#SERVER}' : servicename})
#		else:
#			data[stype] = [{'{#SERVER}' : servicename}]
#	zbx_data = {'request': 'active checks', 'host' : host}
#	for s in mctx['zbx_servers']:
#		sp = s.split(':')
#		server = sp[0]
#		port = 0
#		if len(sp) > 1:
#			try:
#				port = int(sp[1])
#			except ValueError, e:
#				log.error("Port '%s' is not integer" %(sp[1]))
#				continue
#		if port <= 0:
#			port = ZBX_DEF_SERVER_PORT
#		now = time.time()
#		try:
#			try:
#				zbx_send_data(log, server, port, connect_timeout, timeout, zbx_data)
#			finally:
#				use_time = time.time() - now
#		except ZC_Error, e:
#			log.error('zbx_host %s %s "%s:%d" %.6fs %s' %(task['task_info'], stype, server, port, use_time, e))
#			continue
#		log.info('zbx_host %s %s "%s:%d" %.6fs' %(task['task_info'], stype, server, port, use_time))
#		continue
#	return ZC_OK

#def attach_active_task(log, tasks, conf):
#	mctx = conf['modules'][module.ctx_index]
#	if not mctx['zbx_active']:
#		return ZC_IGNORE
#	print 'here'
#	print mctx
#	tasks.append(
#		{
#			'task_info' : '"%s|%s"' %(conf['group'], conf['host']),
#			'service_task' : None,
#			'process_task' : active_send_handler,
#			'interval' : mctx['zbx_active_interval'],
#			'record_handlers' : None,
#			'conf' : conf,
#		})
#	return ZC_OK

def zbx_resend(log, task):
	global unsend_list_file
	global unsend_dir
	if not os.path.isfile(unsend_list_file):
		return ZC_OK
	try:
		unsend_list_lock.lock(unsend_list_lock.LOCK_EX)
		unsend_list = unsend_list_getall(unsend_list_file)
		unsend_list_lock.unlock()
	except Exception, e:
		log.error('%s %s' %(task['task_info'], e))
		return ZC_ERROR
	removes = []
	for file in unsend_list:
		if not os.path.exists(file):
			removes.append(file)
			continue
		try:
			data = read_tempfile(file)
		except Exception, e:
			log.error("%s %s" %(task['task_info'], e))
			continue
		now = time.time()
		try:
			try:
				zbx_send_data(log, data['server'], data['port'], data['connect_timeout'], data['timeout'], data['data'])
				os.unlink(file)
				removes.append(file)
			finally:
				use_time = time.time() - now
		except Exception, e:
			log.error('%s "%s:%d" %.6fs %s' %(task['task_info'], data['server'], data['port'], use_time, e))
			continue
		time.sleep(0.1)
	try:
		unsend_list_del(unsend_list_file, removes)
	except Exception, e:
		log.error("%s %s" %(task['task_info'], e))
	return ZC_OK


def init_master(log, conf, tasks):
	global unsend_list_lock
	global unsend_dir
	mctx = conf['modules'][module.ctx_index]	
	interval = mctx['zbx_resend_interval']
	lockfile = mctx['zbx_unsend_lockfile']
	unsend_dir = mctx['zbx_unsend_dir']
	tasks.append(
		{
			'task_info' : '"resend"',
			'service_task' : None,
			'process_task' : zbx_resend,
			'interval' : interval,
			'record_handlers' : None,
			'conf' : conf,
		}
	)
	try:
		unsend_list_lock = zc_lock.locked(lockfile)
	except zc_lock.error, e:
		cf.log.error("Can't create lock file %s: %s" %(lockfile, e))
		return ZC_ERROR
	return ZC_OK


commands = [
	zc_command(
	name = 'zbx',
	type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_flag_slot,
	describe = 'Zabbix flag',
	key = 'zbx',
	),
	#zc_command(
	#name = 'zbx_active',
	#type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	#set = zc_conf_set_flag_slot,
	#describe = 'Zabbix active check flag',
	#key = 'zbx_active',
	#),
	#zc_command(
	#name = 'zbx_active_interval',
	#type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	#set = zc_conf_set_sec_slot,
	#describe = 'Zabbix active check interval',
	#key = 'zbx_active_interval',
	#),
	zc_command(
	name = 'zbx_resend_interval',
	type = ZC_CHECKER_MAIN_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_sec_slot,
	describe = 'Zabbix resend interval',
	key = 'zbx_resend_interval',
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
	set = zc_conf_set_str_slot,
	describe = 'Zabbix host',
	key = 'zbx_host',
	),
	zc_command(
	name = 'zbx_connect_timeout',
	type =  ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_sec_slot,
	describe = 'Zabbix connect timeout',
	key = 'zbx_connect_timeout',
	),
	zc_command(
	name = 'zbx_timeout',
	type =  ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_sec_slot,
	describe = 'Zabbix timeout',
	key = 'zbx_timeout',
	),
	zc_command(
	name = 'zbx_suffix',
	type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
	set = zc_conf_set_flag_slot,
	describe = 'Zabbix service suffix',
	key = 'zbx_suffix',
	),

]

zbx_ctx = zc_checker_module_ctx(
	service_types = [],
	host_record = host_send_handler,
	service_record = service_send_handler,
	merge_main_conf = merge_main_conf,
	merge_group_conf = merge_group_conf,
	merge_host_conf = merge_host_conf,
	merge_service_conf = merge_service_conf,
#	host_attach_handlers = [attach_active_task]
	)


module = zc_module(
	name = 'zabbix',
	version = '0.1.0',
	type = ZC_CHECKER_MODULE,
	commands = commands,
	ctx = zbx_ctx,
	init_master = init_master,
	)


