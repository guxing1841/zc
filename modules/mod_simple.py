"""\
Simple Module
Copyright (C) Zhou Changrong
"""
import os, sys, time, MySQLdb
import json
from zc_config import *
from zc_core_h import *
from zc_simple_h import *
SIMPLE_DEF_TASK_INTERVAL = 30
task_types = {}
def merge_main_conf(cf, parent, child):
	ctx = child['ctx']
	pctx = parent['ctx']
	for m in cf.child_modules['simple']:
		if m.ctx != None and m.ctx.merge_main_conf != None:
			rc = m.ctx.merge_main_conf(cf, parent, child)
			if rc == ZC_IGNORE:
				ctx['disable'] = True
				return ZC_IGNORE
			elif rc != ZC_OK:
				return ZC_ERROR
	for tconf in child['tasks']:
		if merge_task_conf(cf, child, tconf) == ZC_ERROR:
			return ZC_ERROR
	return ZC_OK

def merge_task_conf(cf, parent, child):
	ctx = child['ctx']
	pctx = parent['ctx']
	if 'disable' in ctx and ctx['disable']:
		return ZC_IGNORE
	keys = pctx['sets'].keys()
	zc_dict_set_no_has(ctx['sets'], pctx['sets'], *keys)
	zc_dict_set_no_has(ctx, pctx, 'task_interval')
	ctx['task_interval'] = zc_dict_get_gt(ctx, 'task_interval', 0, SIMPLE_DEF_TASK_INTERVAL)
	for m in cf.child_modules['simple']:
		if m.ctx != None and m.ctx.merge_task_conf != None:
			rc = m.ctx.merge_task_conf(cf, parent, child)
			if rc == ZC_IGNORE:
				ctx['disable'] = True
				return ZC_OK
			elif rc != ZC_OK:
				return ZC_ERROR
	return ZC_OK



def simple_block(cf, cmd, conf):
	pconf = cf.conf
	ptype = cf.type
	cf.type = ZC_SIMPLE_MAIN_CONF
	ctx_index = cf.cmd.ctx_index
	if pconf['modules'][ctx_index] != None:
		cf.log.error("Simple block is already defined in %s at line %d: %d" %(cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
		return ZC_ERROR
	cf.conf = {
		'start_line' : cf.cur['start_line'], 
		'conf_file' : cf.cur['conf_file'], 
		'modules' : [],
		'tasks' : [],
		'task_indexes' : {},
		'ctx'  : {
			'defines' : {},
			'sets' : {},
			}
		}
	pconf['modules'][ctx_index] = cf.conf
	cf.child_modules['simple'] = []
	i = 0
	for m in cf.modules:
		if m.type == ZC_SIMPLE_MODULE:
			for cmd in m.commands:
				cmd.ctx_index = i
			m.ctx_index = i
			cf.child_modules['simple'].append(m)
			cf.conf['modules'].append({})
			if m.ctx != None and m.ctx.task_types != None:
				for t in m.ctx.task_types:
					if t.lower() in task_types:
						cf.log.error("Task type '%s' is already defined in %s at line %d\n" %(t, cf.cur['conf_file'], cf.cur['start_line']))
						return ZC_ERROR
					else:
						task_types[t.lower()] = i
			i += 1

	for key in pconf['ctx']['defines'].keys():
		cf.conf['ctx']['defines'][key] = pconf['ctx']['defines'][key]
	if cf.file_parser() == ZC_ERROR:
		return ZC_ERROR
	if merge_main_conf(cf, pconf, cf.conf) != ZC_OK:
		return ZC_ERROR
	del cf.conf['ctx']['defines']
	cf.conf = pconf
	cf.type = ptype
	return ZC_OK

def task_block(cf, cmd, conf):
	name = cf.args[1]
	context = cf.conf['tasks']
	indexes = cf.conf['task_indexes']
	if name in indexes:
		cf.log.error("Task '%s' is already defined in %s at line %d\n" %(name, cf.cur['conf_file'], cf.cur['start_line']))
		return ZC_ERROR
	pconf = cf.conf
	ptype = cf.type
	context.append({
		'start_line' : cf.cur['start_line'], 
		'conf_file' : cf.cur['conf_file'], 
		'task' : name,
		'modules' : [],
		'ctx' : {
			'defines' : {'task' : name},
			'sets' : {'task' : name},
			}
		})
	indexes[name] = len(context) - 1
	cf.conf = context[-1]
	cf.type = ZC_SIMPLE_TASK_CONF
	i = 0
	for m in cf.child_modules['checker']:
		cf.conf['modules'].append({})
		i += 1
	for key in pconf['ctx']['defines']:
		cf.conf['ctx']['defines'][key] = pconf['ctx']['defines'][key]
	if cf.file_parser() == ZC_ERROR:
		return ZC_ERROR
	del cf.conf['ctx']['defines']
	if 'task_type' not in cf.conf['ctx']:
		cf.log.error("Task '%s' task_type is not set in %s at line %d\n" %(name, cf.cur['conf_file'], cf.cur['start_line']))
		return ZC_ERROR
	cf.conf = pconf
	cf.type = ptype
	return ZC_OK


def init_master(cf, conf, tasks):
	if 'simple' not in cf.child_modules:
		cf.log.error("Checker block not set, module checker init master faild\n")
		return ZC_ERROR
	for m in cf.child_modules['simple']:
		if m.init_master != None:
			if m.init_master(cf, conf, tasks) == ZC_ERROR:
				return ZC_ERROR
	ctx = conf['ctx']
	for tconf in conf['tasks']:
		tctx = tconf['ctx']
		if 'disable' in tctx and tctx['disable']:
			continue
		ttype = tctx['task_type']
		task_module = cf.child_modules['simple'][task_types[ttype]]
		tasks.append(
			{
				'task_info' : '"simple %s"' %(tconf['task']),
				'process_task' : task_module.ctx.process_handler,
				'interval' : tctx['task_interval'],
				'conf' : tconf,
			})
	return ZC_OK

def zc_set_task_type(cf, cmd, conf):
	global task_types
	var = cf.args[1].lower()
	if var not in task_types:
		cf.log.error("Task type '%s' is not supported in %s at line %d\n" %(cf.args[1], cf.cur['conf_file'], cf.cur['line']))
		return ZC_ERROR
	return zc_conf_set_str_slot(cf, cmd, conf)

	
commands = [
	zc_command(
		name = 'simple',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_BLOCK|ZC_CONF_NOARGS,
		set = simple_block,
		describe = 'Simple block',
		key= None,
		),

	zc_command(
		name = 'task',
		type = ZC_SIMPLE_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_BLOCK|ZC_CONF_TAKE1,
		set = task_block,
		describe = 'Task block',
		key = None,
	),
	zc_command(
		name = 'task_type',
		type = ZC_SIMPLE_TASK_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_set_task_type,
		describe = 'Task type',
		key = 'task_type',
	),
	zc_command(
		name = 'task_interval',
		type = ZC_SIMPLE_MAIN_CONF|ZC_SIMPLE_TASK_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		describe = 'Task interval',
		key = 'task_interval',
	),



	
]

simple_ctx = zc_simple_module_ctx(
	merge_main_conf = merge_main_conf,
	merge_task_conf = merge_task_conf,
	)

module = zc_module(
	name = 'simple',
	version = '0.1.0',
	type = ZC_CORE_MODULE,
	commands = commands,
	ctx = simple_ctx,
	init_master = init_master,
	)
