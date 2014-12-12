"""\
COMMAND module
Copyright (C) Zhou Changrong
"""
import os, sys, time, socket, types, subprocess, select, signal, errno
try:
	import json
except ImportError, e:
	import simplejson as json
from zc_config_h import *
from zc_core_h import *
from zc_checker_h import *
import zc_command as cmd

COMMAND_DEF_TIMEOUT = 15
COMMAND_DEF_TYPE = 'text'
command_types = ['text', 'json']

def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'command_timeout')
	zc_dict_set_no_has(ctx, pctx, 'command_type')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'command_timeout')
	zc_dict_set_no_has(ctx, pctx, 'command_type')
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	stype = child['ctx']['type']
	zc_dict_set_no_has(ctx, pctx, 'command_timeout')
	zc_dict_set_no_has(ctx, pctx, 'command_type')
	ctx['command_timeout'] = zc_dict_get_ge(ctx, 'command_timeout', 0, COMMAND_DEF_TIMEOUT)
	ctx['command_type'] = zc_dict_get_gt(ctx, 'command_type', 0, COMMAND_DEF_TYPE)
	if stype in module.ctx.service_types:
		if 'command' not in ctx:
			cf.log.warn('command is not set in block "%s" will disabled "%s"in line %d in %s' %(child['service'], stype, child['start_line'], child['conf_file']))
			return ZC_IGNORE
	return ZC_OK

def loadtext(text):
	pattern1 = re.compile(r"\r?\n")
	lines = pattern1.split(text)
	ret = []
	pattern2 = re.compile(r"^\s+|\s+$")
	pattern3 = re.compile(r"^(.+?)(\s+(.+?))?$")
	for line in lines:
		ln = pattern2.sub("", line)
		if ln == "":
			continue
		m = pattern3.match(ln)
		g = m.groups()
		key = g[0]
		value = g[2]
		ret.append({'key' : key, 'value': value})
	return ret


def task_handler(log, task):
	conf = task['conf']
	mctx = conf['modules'][module.ctx_index]
	command = mctx['command']
	stype = conf['ctx']['type']
	timeout = mctx['command_timeout']
	now = time.time()
	try:
		try:
			out, err = cmd.call(command, timeout)
			if len(err):
				log.error('%s "%s" "%s"' %(task['task_info'], mctx['command'], err))
		finally:
			use_time = time.time() - now
	except cmd.error, e:
		log.error('%s "%s" %.6fs "%s"' %(task['task_info'], mctx['command'], use_time, e))
		return None
	if mctx['command_type'] == 'text': 
		status = loadtext(out)
	elif mctx['command_type'] == 'json': 
		try:
			status = json.loads(out)
		except ValueError, e:
			log.error('%s "%s" %.6fs "can\'t parse result: %s"' %(task['task_info'], mctx['command'], use_time, e))
			return None
	use_time = time.time() - now
	log.info('%s "%s" %.6fs' %(task['task_info'], mctx['command'], use_time))
	return status


commands = [
	zc_command(
		name = 'command',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'command',
		describe = 'command'
	),
	zc_command(
		name = 'command_type',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		key = 'command_type',
		describe = 'Command type'
	),
	zc_command(
		name = 'command_timeout',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		key = 'command_timeout',
		describe = 'Command timeout'
	),
]

command_ctx = zc_checker_module_ctx(
	service_types = ['command'],
	service_task = task_handler,
	merge_main_conf = merge_main_conf,
	merge_group_conf = merge_group_conf,
	merge_host_conf = merge_host_conf,
	merge_service_conf = merge_service_conf
	)
	

module = zc_module(
	name = 'command',
	version = '0.1.0',
	type = ZC_CHECKER_MODULE,
	commands = commands,
	ctx = command_ctx
	)
