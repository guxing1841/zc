"""\
MySQL Module
Copyright (C) Zhou Changrong
"""
import os, sys, time, MySQLdb
from zc_config import *
from zc_core_h import *
from zc_checker_h import *
MYSQL_DEF_PORT = 3306
MYSQL_DEF_CONNECT_TIMEOUT = 10
MYSQL_DEF_USER = 'root'
MYSQL_DEF_PASSWD = ''
MYSQL_DEF_SLAVE = True

def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'mysql_port', 'mysql_connect_timeout', 'mysql_user', 'mysql_passwd', 'mysql_slave')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'mysql_port', 'mysql_connect_timeout', 'mysql_user', 'mysql_passwd', 'mysql_slave')
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	stype = child['ctx']['type']
	zc_dict_set_no_has(ctx, pctx, 'mysql_port', 'mysql_connect_timeout', 'mysql_user', 'mysql_passwd', 'mysql_slave')
	ctx['mysql_port'] = zc_dict_get_gt(ctx, 'mysql_port', 0, MYSQL_DEF_PORT)
	ctx['mysql_connect_timeout'] = zc_dict_get_ge(ctx, 'mysql_connect_timeout', 0, MYSQL_DEF_CONNECT_TIMEOUT)
	ctx['mysql_user'] = ctx.get('mysql_user', MYSQL_DEF_USER)
	ctx['mysql_passwd'] = ctx.get('mysql_passwd', MYSQL_DEF_PASSWD)
	ctx['mysql_slave'] = ctx.get('mysql_slave', MYSQL_DEF_SLAVE)
	if stype in module.ctx.service_types:
		if 'mysql_host' not in ctx:
			cf.log.warn('mysql_host is not set in block "%s" will disabled "%s" in line %d in %s' %(child['service'], stype, child['start_line'], child['conf_file']))
			return ZC_IGNORE
	return ZC_OK

def task_handler(log, task):
	conf = task['conf']
	mctx = conf['modules'][module.ctx_index]
	stype = conf['ctx']['type']
	now = time.time()
	status = {}
	timeout = int(mctx['mysql_connect_timeout'])
	try:
		kwargs = {
			'host'            : mctx['mysql_host'],
			'port'            : mctx['mysql_port'],
			'user'            : mctx['mysql_user'],
			'passwd'          : mctx['mysql_passwd'],
			'unix_socket'     : mctx['mysql_socket']
		}
		if timeout > 0:
			kwargs['connect_timeout'] = timeout
		conn = MySQLdb.connect(**kwargs)
		cursor = conn.cursor()
		cursor.execute("SHOW GLOBAL STATUS")
		results = cursor.fetchall()
		for r in results:
			status[r[0]] = r[1]
		if mctx['mysql_slave']:
			cursor.execute("SHOW SLAVE STATUS")
			result = cursor.fetchone()
			if result != None:
				num_fields = len(cursor.description)
				i = 0
				while i < num_fields:
					if cursor.description[i][0] not in status:
						status[cursor.description[i][0]] = result[i]
						i+=1
			else:
				log.debug("'SHOW SLAVE STATUS' is unavailable")
		cursor.execute("SHOW VARIABLES")
		results = cursor.fetchall()
		for r in results:
			if r[0] not in status:
				status[r[0]] = r[1]
		cursor.close()
		conn.close()
		status['ping'] = 1
	except MySQLdb.Error,e:
		use_time = time.time() - now
		log.error('%s "%s@%s:%d" %.6fs "errno %d: %s"' %(task['task_info'], stype, mctx['mysql_user'], mctx['mysql_host'], mctx['mysql_port'], use_time, e.args[0], e.args[1]))
		status['ping'] = 0
		return status
	use_time = time.time() - now
	log.info('%s "%s@%s:%d" %.6fs' %(task['task_info'], mctx['mysql_user'], mctx['mysql_host'], mctx['mysql_port'], use_time))
	r = []
	for key in status:
		r.append({'key' : key, 'value' : status[key]})
	return r

commands = [
	zc_command(
		name = 'mysql_host',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'mysql_host',
		describe = 'Mysql host'
	),
	zc_command(
		name = 'mysql_port',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_int_slot,
		key = 'mysql_port',
		describe = 'Mysql port'
	),
	zc_command(
		name = 'mysql_user',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'mysql_user',
		describe = 'Mysql user'
	),
	zc_command(
		name = 'mysql_passwd',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'mysql_passwd',
		describe = 'Mysql password'
	),
	zc_command(
		name = 'mysql_socket',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'mysql_socket',
		describe = 'Mysql unix socket path'
	),
	zc_command(
		name = 'mysql_slave',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_flag_slot,
		key = 'mysql_slave',
		describe = 'Mysql slave'
	),
	zc_command(
		name = 'mysql_connect_timeout',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		key = 'mysql_connect_timeout',
		describe = 'Mysql connect timeout'
	),


]

mysql_ctx = zc_checker_module_ctx(
	service_types = ['mysql'],
	service_task = task_handler,
	merge_main_conf = merge_main_conf,
	merge_group_conf = merge_group_conf,
	merge_host_conf = merge_host_conf,
	merge_service_conf = merge_service_conf,
	)

module = zc_module(
	name = 'mysql',
	version = '0.1.0',
	type = ZC_CHECKER_MODULE,
	commands = commands,
	ctx = mysql_ctx
	)
