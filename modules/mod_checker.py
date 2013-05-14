"""\
Checker core module
Copyright (C) Zhou Changrong
"""
import os, sys
from zc_config import *
from zc_core_h import *
from zc_checker_h import *

service_types = {}

CHECKER_DEF_HOST_INTERVAL    = 3600
CHECKER_DEF_SVC_INTERVAL     = 60
def merge_main_conf(cf, parent, child):
	ctx = child['ctx']
	pctx = parent['ctx']
	for m in cf.child_modules['checker']:
		if m.ctx != None and m.ctx.merge_main_conf != None:
			if m.ctx.merge_main_conf(cf, parent, child) != ZC_OK:
				return ZC_ERROR
	for gconf in child['groups']:
		if merge_group_conf(cf, child, gconf) != ZC_OK:
			return ZC_ERROR
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['ctx']
	pctx = parent['ctx']
	if 'disable' in ctx and ctx['disable']:
		return ZC_IGNORE
	keys = pctx['sets'].keys()
	zc_dict_set_no_has(ctx['sets'], pctx['sets'], *keys)
	zc_dict_set_no_has(ctx, pctx, 'host_interval', 'service_interval', 'active_servers')
	for m in cf.child_modules['checker']:
		if m.ctx != None and m.ctx.merge_group_conf != None:
			rc = m.ctx.merge_group_conf(cf, parent, child)
			if rc == ZC_IGNORE:
				ctx['disable'] = True
				return ZC_OK
			elif rc != ZC_OK:
				return ZC_ERROR
	for hconf in child['hosts']:
		if merge_host_conf(cf, child, hconf) == ZC_ERROR:
			return ZC_ERROR
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['ctx']
	pctx = parent['ctx']
	if 'disable' in ctx and ctx['disable']:
		return ZC_IGNORE
	keys = pctx['sets'].keys()
	zc_dict_set_no_has(ctx['sets'], pctx['sets'], *keys)
	zc_dict_set_no_has(ctx, pctx, 'host_interval', 'service_interval', 'active_servers')
	ctx['host_interval'] = zc_dict_get_gt(ctx, 'host_interval', 0, CHECKER_DEF_HOST_INTERVAL)
	for m in cf.child_modules['checker']:
		if m.ctx != None and m.ctx.merge_host_conf != None:
			rc = m.ctx.merge_host_conf(cf, parent, child)
			if rc == ZC_IGNORE:
				ctx['disable'] = True
				return ZC_OK
			elif rc != ZC_OK:
				return ZC_ERROR
	for sconf in child['services']:
		if merge_service_conf(cf, child, sconf) == ZC_ERROR:
			return ZC_ERROR
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['ctx']
	pctx = parent['ctx']
	if 'disable' in ctx and ctx['disable']:
		return ZC_IGNORE
	keys = pctx['sets'].keys()
	zc_dict_set_no_has(ctx['sets'], pctx['sets'], *keys)
	zc_dict_set_no_has(ctx, pctx, 'host_interval', 'service_interval', 'active_servers')
	ctx['service_interval'] = zc_dict_get_gt(ctx, 'service_interval', 0, CHECKER_DEF_SVC_INTERVAL)
	for m in cf.child_modules['checker']:
		if m.ctx != None and m.ctx.merge_service_conf != None:
			rc = m.ctx.merge_service_conf(cf, parent, child)
			if rc == ZC_IGNORE:
				ctx['disable'] = True
				return ZC_IGNORE
			elif rc != ZC_OK:
				return ZC_ERROR
	return ZC_OK


def checker_block(cf, cmd, conf):
	global service_types
	pconf = cf.conf
	ptype = cf.type
	cf.type = ZC_CHECKER_MAIN_CONF
	ctx_index = cf.cmd.ctx_index
	if pconf['modules'][ctx_index] != None:
		cf.log.error("Checker block is already defined in %s at line %d: %d" %(cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
		return ZC_ERROR
	cf.conf = {
		'start_line' : cf.cur['start_line'], 
		'conf_file' : cf.cur['conf_file'], 
		'modules' : [],
		'groups' : [],
		'group_indexes' : {},
		'ctx'  : {
			'defines' : {},
			'sets' : {},
			}
		}
	pconf['modules'][ctx_index] = cf.conf
	cf.child_modules['checker'] = []
	i = 0
	for m in cf.modules:
		if m.type == ZC_CHECKER_MODULE:
			for cmd in m.commands:
				cmd.ctx_index = i
			m.ctx_index = i
			cf.child_modules['checker'].append(m)
			cf.conf['modules'].append({})
			if m.ctx != None and m.ctx.service_types != None:
				for t in m.ctx.service_types:
					if t.lower() in service_types:
						cf.log.error("Service type '%s' is already defined in %s at line %d\n" %(t, cf.cur['conf_file'], cf.cur['start_line']))
						return ZC_ERROR
					else:
						service_types[t.lower()] = i
		
			i += 1
	for key in pconf['ctx']['defines'].keys():
		cf.conf['ctx']['defines'][key] = pconf['ctx']['defines'][key]
	if cf.file_parser() == ZC_ERROR:
		return ZC_ERROR
	if merge_main_conf(cf, pconf, cf.conf) != ZC_OK:
		return ZC_ERROR
	del cf.conf['ctx']['defines']
	del cf.conf['group_indexes']
	cf.conf = pconf
	cf.type = ptype
	return ZC_OK

def group_block(cf, cmd, conf):
	name = cf.args[1]
	context = cf.conf['groups']
	indexes = cf.conf['group_indexes']
	if name in indexes:
		cf.log.error("Group '%s' is already defined in %s at line %d\n" %(name, cf.cur['conf_file'], cf.cur['start_line']))
		return ZC_ERROR
	pconf = cf.conf
	ptype = cf.type
	context.append({ 
		'start_line' : cf.cur['start_line'], 
		'conf_file' : cf.cur['conf_file'], 
		'group' : name,
		'modules' : [],
		'hosts' : [],
		'host_indexes' : {},
		'ctx' : {
			'defines' : {'group' : name},
			'sets' : {'group' : name},
			}
		})
	indexes[name] = len(context) - 1
	cf.conf = context[-1]
	cf.type = ZC_CHECKER_GROUP_CONF
	i = 0
	for m in cf.child_modules['checker']:
		cf.conf['modules'].append({})
		i += 1
	for key in pconf['ctx']['defines'].keys():
		cf.conf['ctx']['defines'][key] = pconf['ctx']['defines'][key]
	if cf.file_parser() == ZC_ERROR:
		return ZC_ERROR
	del cf.conf['ctx']['defines']
	del cf.conf['host_indexes']
	cf.conf = pconf
	cf.type = ptype
	return ZC_OK



def host_block(cf, cmd, conf):
	name = cf.args[1]
	context = cf.conf['hosts']
	indexes = cf.conf['host_indexes']
	if name in indexes:
		cf.log.error("Host '%s' is already defined in %s at line %d\n" %(name, cf.cur['conf_file'], cf.cur['start_line']))
		return ZC_ERROR
	pconf = cf.conf
	ptype = cf.type
	context.append({
		'start_line' : cf.cur['start_line'], 
		'conf_file' : cf.cur['conf_file'], 
		'group' : pconf['group'],
		'host' : name,
		'modules' : [],
		'services' : [],
		'service_indexes' : {},
		'ctx' : {
			'defines' : {'host' : name},
			'sets' : {'host' : name},
			}
		})
	indexes[name] = len(context) - 1
	cf.conf = context[-1]
	cf.type = ZC_CHECKER_HOST_CONF
	i = 0
	for m in cf.child_modules['checker']:
		cf.conf['modules'].append({})
		i += 1
	for key in pconf['ctx']['defines'].keys():
		cf.conf['ctx']['defines'][key] = pconf['ctx']['defines'][key]
	if cf.file_parser() == ZC_ERROR:
		return ZC_ERROR
	del cf.conf['ctx']['defines']
	del cf.conf['service_indexes']
	cf.conf = pconf
	cf.type = ptype
	return ZC_OK


def service_block(cf, cmd, conf):
	name = cf.args[1]
	context = cf.conf['services']
	indexes = cf.conf['service_indexes']
	if name in indexes:
		cf.log.error("Host '%s' is already defined in %s at line %d\n" %(name, cf.cur['conf_file'], cf.cur['start_line']))
		return ZC_ERROR
	pconf = cf.conf
	ptype = cf.type
	context.append({ 
		'start_line' : cf.cur['start_line'], 
		'conf_file' : cf.cur['conf_file'], 
		'group' : pconf['group'],
		'host' : pconf['host'],
		'service' : name,
		'modules' : [],
		'ctx' : {
			'defines' : {'service' : name},
			'sets' : {'service' : name},
			}
		})
	indexes[name] = len(context) - 1
	cf.conf = context[-1]
	cf.type = ZC_CHECKER_SVC_CONF
	i = 0
	for m in cf.child_modules['checker']:
		cf.conf['modules'].append({})
		i += 1
	for key in pconf['ctx']['defines'].keys():
		cf.conf['ctx']['defines'][key] = pconf['ctx']['defines'][key]
	if cf.file_parser() == ZC_ERROR:
		return ZC_ERROR
	del cf.conf['ctx']['defines']
	if 'type' not in cf.conf['ctx']:
		cf.log.error("Server '%s' type is not set in %s at line %d\n" %(name, cf.cur['conf_file'], cf.cur['start_line']))
		return ZC_ERROR
	cf.conf = pconf
	cf.type = ptype
	return ZC_OK


def zc_set_svc_type(cf, cmd, conf):
	global service_types
	var = cf.args[1].lower()
	if var not in service_types:
		cf.log.error("Service type '%s' is not supported in %s at line %d\n" %(cf.args[1], cf.cur['conf_file'], cf.cur['line']))
		return ZC_ERROR
	return zc_conf_set_str_slot(cf, cmd, conf)


def host_process_task(log, t):
	for h in t['record_handlers']:
		h(log, t)
	return ZC_OK

def in_list(t, d):
	for v in t:
		if len(v) > 0:
			if v[0:1] == '~':
				if re.match(v[1:], d):
					return True
		if v == d:
			return True
	return False
	

def service_process_task(log, t):
	rc = t['service_task'](log, t)
	if rc == None:
		return ZC_OK
	ignore_list = t['conf']['ctx'].get('service_record_ignore')
	match_list = t['conf']['ctx'].get('service_record_match')
	if ignore_list != None or match_list != None:
		for key in rc.keys():
			if ignore_list != None and in_list(ignore_list, key):
				del(rc[key])
			if match_list != None and not in_list(match_list, key):
				del(rc[key])
	for h in t['record_handlers']:
		h(log, t, rc)
	return ZC_OK


def init_master(cf, conf, tasks):
	host_record_handlers = []
	service_record_handlers = []
	if 'checker' not in cf.child_modules:
		cf.log.error("Checker block not set, moduler checker init master faild\n")
		return ZC_ERROR
		
	for m in cf.child_modules['checker']:
		if m.ctx == None:
			continue
		if m.ctx.host_record != None:
			host_record_handlers.append(m.ctx.host_record)
	for m in cf.child_modules['checker']:
		if m.ctx == None:
			continue
		if m.ctx.service_record!= None:
			service_record_handlers.append(m.ctx.service_record)
	ctx = conf['ctx']
	for gconf in conf['groups']:
		gctx = gconf['ctx']
		if 'disable' in gctx and gctx['disable']:
			continue
		for hconf in gconf['hosts']:
			hctx = hconf['ctx']
			if 'disable' in hctx and hctx['disable']:
				continue
			t = CHECKER_DEF_HOST_INTERVAL
			if 'host_interval' in hctx:
				t = hctx['host_interval']
			tasks.append(
				{
					'task_info' : '"%s|%s"' %(hconf['group'], hconf['host']),
					'service_task' : None,
					'process_task' : host_process_task,
					'interval' : t,
					'record_handlers' : host_record_handlers,
					'conf' : hconf,
				})
			for sconf in hconf['services']:
				sctx = sconf['ctx']
				if 'disable' in sctx and sctx['disable']:
					continue
				stype = sctx['type']
				service_module = cf.child_modules['checker'][service_types[stype]]
				t = CHECKER_DEF_SVC_INTERVAL
				if 'service_interval' in sctx:
					x = sctx['service_interval']
				tasks.append(
					{
						'task_info' : '"%s|%s|%s" %s' %(sconf['group'], sconf['host'], sconf['service'], sconf['ctx']['type']),
						'service_task' : service_module.ctx.service_task,
						'process_task' : service_process_task,
						'interval' : t,
						'record_handlers' : service_record_handlers,
						'conf' : sconf
					})
	return ZC_OK

commands = [
	zc_command(
		name = 'checker',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_BLOCK|ZC_CONF_NOARGS,
		set = checker_block,
		describe = 'Checker block',
		key= None,
		),
	zc_command(
		name = 'host_group',
		type = ZC_CHECKER_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_BLOCK|ZC_CONF_TAKE1,
		set = group_block,
		describe = 'Host group',
		key = None,
	),
	zc_command(
		name = 'host',
		type = ZC_CHECKER_GROUP_CONF|ZC_CONF_DIRECT|ZC_CONF_BLOCK|ZC_CONF_TAKE1,
		set = host_block,
		describe = 'Host',
		key = None,
	),
	zc_command(
		name = 'service',
		type = ZC_CHECKER_HOST_CONF|ZC_CONF_DIRECT|ZC_CONF_BLOCK|ZC_CONF_TAKE1,
		set = service_block,
		describe = 'Service',
		key = None,
	),
	zc_command(
		name = 'type',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_set_svc_type,
		describe = 'Service type',
		key = 'type',
	),
	zc_command(
		name = 'host_interval',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		describe = 'Host interval',
		key = 'host_interval',
	),
	zc_command(
		name = 'service_interval',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		describe = 'Service interval',
		key = 'service_interval',
	),
	zc_command(
		name = 'service_record_match',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_DIRECT|ZC_CONF_1MORE|ZC_CONF_MULTI,
		set = None,
		describe = 'Record match filter',
		key = 'service_record_match',
	),
	zc_command(
		name = 'service_record_ignore',
		type = ZC_CHECKER_SVC_CONF|ZC_CONF_DIRECT|ZC_CONF_1MORE|ZC_CONF_MULTI,
		set = None,
		describe = 'Records ignore filter',
		key = 'service_record_ignore',
	),
]

module = zc_module(
	name = 'checker',
	version = '0.2',
	type = ZC_CORE_MODULE,
	commands = commands,
	ctx = None,
	init_master = init_master,
	)
