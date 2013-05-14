"""\
system module
Copyright (C) Zhou Changrong
"""
import os, sys, time, json
from zc_config import *
from zc_core_h import *
from zc_checker_h import *

def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'system_args')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'system_args')
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	stype = child['ctx']['type']
	zc_dict_set_no_has(ctx, pctx, 'system_args')
	return ZC_OK

def get_loadavg(log):
	filename = "/proc/loadavg"
	prefix = "loadavg"
	try:
		f = open(filename, "r")
		lines = f.readlines()
		f.close()
	except IOError, e:
		log.error("Can't open %s: %s" %(filename, e))
		return None
	lstat = lines[0].split()
	status = {}
	status[prefix + '.1'] = lstat[0] 
	status[prefix + '.5'] = lstat[1] 
	status[prefix + '.15'] = lstat[2] 
	process = lstat[3].split("/")
	status[prefix + '.running'] = process[0]
	status[prefix + '.total'] = process[1]
	status[prefix + '.last'] = lstat[4]
	return status

def get_stat(log):
	filename = "/proc/stat"
	prefix = 'stat'
	try:
		f = open(filename, "r")
		lines = f.readlines()
		f.close()
	except IOError, e:
		log.error("Can't open %s: %s" %(filename, e))
		return None
	cpu_fields = ('user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq')
	status = {}
	status["%s.cpus" %(prefix)] = 0
	for line in lines:
		lstat = line.split()
		if (lstat[0] == "cpu"):
			i = 1
			for field in cpu_fields:
				status["%s.cpu.%s" %(prefix, field)] = lstat[i]
		elif(lstat[0][0:-1] == "cpu"):
			status["%s.cpus" %(prefix)] += 1 
		else:
			status["%s.%s" %(prefix, lstat[0])] = lstat[1]
	return status

def get_meminfo(log):
	filename = "/proc/meminfo"
	prefix = 'meminfo'
	try:
		f = open(filename, "r")
		lines = f.readlines()
		f.close()
	except IOError, e:
		log.error("Can't open %s: %s" %(filename, e))
		return None
	status = {}
	for line in lines:
		lstat = line.split()
		status["%s.%s" %(prefix, lstat[0][0:-1])] = lstat[1]
	return status

def get_sockstat(log):
	filename = "/proc/net/sockstat"
	prefix = 'sockstat'
	try:
		f = open(filename, "r")
		lines = f.readlines()
		f.close()
	except IOError, e:
		log.error("Can't open %s: %s" %(filename, e))
		return None
	status = {}
	for line in lines:
		lstat = line.split()
		i = 1
		while (i<len(lstat)-1):
			status["%s.%s.%s" %(prefix, lstat[0][0:-1], lstat[i])] = lstat[i+1]
			i+= 2
	return status

def get_netdev(log):
	filename = "/proc/net/dev"
	prefix1 = 'netdev'
	prefix2 = 'if'
	try:
		f = open(filename, "r")
		lines = f.readlines()
		f.close()
	except IOError, e:
		log.error("Can't open %s: %s" %(filename, e))
		return None
	status = {}
	discovery = []
	i = 2
	while (i < len(lines)):
		lstat = lines[i].split()
		ifname = lstat[0][0:-1]
		status["%s.if.%s.in" %(prefix1, ifname)] = lstat[1]
		status["%s.if.%s.out" %(prefix1, ifname)] = lstat[9]
		discovery.append(ifname)
		i += 1
	status["%s.discovery" % (prefix1)] = discovery
	return status

def get_netstat(log):
	filename = "/proc/net/netstat"
	prefix = 'netstat'
	try:
		f = open(filename, "r")
		lines = f.readlines()
		f.close()
	except IOError, e:
		log.error("Can't open %s: %s" %(filename, e))
		return None
	status = {}
	i = 0
	while i<len(lines):
		ltitle = lines[i].split()
		lstat = lines[i+1].split()
		j = 1
		while j < len(ltitle):
			status["%s.%s.%s" %(prefix, ltitle[0][0:-1], ltitle[j])] = lstat[j]
			j += 1
		i += 2 
	return status

def get_snmp(log):
	filename = "/proc/net/snmp"
	prefix = 'snmp'
	try:
		f = open(filename, "r")
		lines = f.readlines()
		f.close()
	except IOError, e:
		log.error("Can't open %s: %s" %(filename, e))
		return None
	status = {}
	i = 0
	while i<len(lines):
		ltitle = lines[i].split()
		lstat = lines[i+1].split()
		j = 1
		while j < len(ltitle):
			status["%s.%s.%s" %(prefix, ltitle[0][0:-1], ltitle[j])] = lstat[j]
			j += 1
		i += 2 
	return status

SYSTEM_ACTIONS = {'loadavg' : [get_loadavg], 'stat' : [get_stat], 'meminfo' : [get_meminfo], 'sockstat' : [get_meminfo], 'netdev' : [get_netdev], 'netstat' : [get_netstat], 'snmp' : [get_snmp]}
SYSTEM_ACTION_GROUPS = {'all' : SYSTEM_ACTIONS.keys()}
	
def get_status(log, *args):
	exists = {}
	status = {}	
	for arg in args:
		if arg in exists:
			continue
		exists[arg] = True
		action_args = []
		if len(SYSTEM_ACTIONS[arg]) > 1:
			action_args = SYSTEM_ACTIONS[arg][1:]
		s = SYSTEM_ACTIONS[arg][0](log, *action_args)
		if s == None:
			continue
		for key in s:
			status[key] = s[key]
	#print json.dumps(status, indent=4)
	return status


def task_handler(log, task):
	conf = task['conf']
	mctx = conf['modules'][module.ctx_index]
	stype = conf['ctx']['type']
	now = time.time()
	status = get_status(log, *mctx['system_args'])
	use_time = time.time() - now
	log.info('%s "localhost" %.6fs' %(task['task_info'], use_time))
	status['_check_use_time_'] = use_time
	return status



def system_set_args_slot(cf, cmd, conf):
	if cmd.key not in conf:
		conf[cmd.key] = []
	for arg in cf.args[1:]:
		larg = arg.lower()
		if larg in SYSTEM_ACTION_GROUPS:
			for a in SYSTEM_ACTION_GROUPS[larg]:
				if a not in conf[cmd.key]:
					conf[cmd.key].append(a)
		elif larg not in SYSTEM_ACTIONS:
			if larg not in conf[cmd.key]:
				conf[cmd.key].append(larg)
		else:
			cf.log.error("Unsport '%s' in %s at line %d: %d" %(arg, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
			return ZC_ERROR
	return ZC_OK



commands = [
	zc_command(
		name = 'system_args',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_1MORE|ZC_CONF_MULTI,
		set = system_set_args_slot,
		key = 'system_args',
		describe = 'Memcache or ttserver host'
	),
]

mc_ctx = zc_checker_module_ctx(
	service_types = ['system'],
	service_task = task_handler,
	merge_main_conf = merge_main_conf,
	merge_group_conf = merge_group_conf,
	merge_host_conf = merge_host_conf,
	merge_service_conf = merge_service_conf,
	)

module = zc_module(
	name = 'system',
	version = '0.1.0',
	type = ZC_CHECKER_MODULE,
	commands = commands,
	ctx = mc_ctx
	)
