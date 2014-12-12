"""\
system module
Copyright (C) Zhou Changrong
"""
import os
import sys
import time
import statvfs
import re
import mmap
import fcntl
import socket
import subprocess
import struct
import zlib

from zc_common import ZC_Error
from zc_config import *
from zc_core_h import *
from zc_checker_h import *
import zc_lock
import zc_command as cmd

SYSTEM_DEF_CPU_LOCKFILE = 'logs/zc_cpu.lock'
SYSTEM_DEF_COMMAND_TIMEOUT = 10
cpushared = None
system_cpu_lockfile = None
cpulock = None
SYSTEM_CPU_SIZE = struct.calcsize('%dq' %(10))
BUFSIZE = 4096


def init_master(cf, conf, tasks):
	global cpushared, cpulock
	mctx = conf['modules'][module.ctx_index]
	lockfile = mctx['system_cpu_lockfile']
	try:
		cpushared = mmap.mmap(-1, SYSTEM_CPU_SIZE, mmap.MAP_SHARED|mmap.MAP_ANONYMOUS, mmap.PROT_READ|mmap.PROT_WRITE)
		cpushared.seek(0)
		cpushared.write(struct.pack('%dq' %10, *([0] * 10)))
	except mmap.error, e:
		cf.log.error("Cpu shared %s" %(e))
		return ZC_ERROR
	try:
		cpulock = zc_lock.locked(lockfile)
	except zc_lock.error, e:
		cf.log.error("Can't create lock file %s: %s" %(lockfile, e))
		return ZC_ERROR
	return ZC_OK
	


def merge_main_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	if 'system_cpu_lockfile' not in ctx:
		ctx['system_cpu_lockfile'] = SYSTEM_DEF_CPU_LOCKFILE
	return ZC_OK

def merge_group_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'system_gets', 'system_commands', 'system_command_timeout')
	return ZC_OK

def merge_host_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	zc_dict_set_no_has(ctx, pctx, 'system_gets', 'system_commands', 'system_command_timeout')
	return ZC_OK

def merge_service_conf(cf, parent, child):
	ctx = child['modules'][module.ctx_index]
	pctx = parent['modules'][module.ctx_index]
	stype = child['ctx']['type']
	zc_dict_set_no_has(ctx, pctx, 'system_gets', 'system_commands', 'system_command_timeout')
	ctx['system_command_timeout'] = zc_dict_get_ge(ctx, 'system_command_timeout', 0, SYSTEM_DEF_COMMAND_TIMEOUT)
	return ZC_OK

def get_filelines(filename, n=0):
	lines = None
	try:
	
		f = open(filename, "r")
	except IOError, e:
		raise ZC_Error, "can't open %s: %s" %(filename, e)
		return None
	try:
		try:
			if n > 0:
				lines = []
				i = 0
				while i < n:
					lines.append(f.readline())
					i += 1
			else:
				lines = f.readlines()
		finally:
			f.close()
	except IOError, e:
		raise ZC_Error, "can't read %s: %s" %(filename, e)
	return lines

def get_ifipaddr(ifname):
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
	except socket.error, e:
		raise ZC_Error, "Can't create socket: %s" %(e)
	ifreq = struct.pack('16sH14s', ifname, 0, '')
	SIOCGIFADDR = 0x8915
	try:
		try:
			ret = fcntl.ioctl(s.fileno(), SIOCGIFADDR, ifreq)
		finally:
			s.close()
	except IOError, e:
		raise ZC_Error, "Can't get %s ip address: %s" %(ifname, e)
	ipaddr_l = struct.unpack('16sH14B', ret)[4:8] 
	return "%d.%d.%d.%d" %(ipaddr_l[0], ipaddr_l[1], ipaddr_l[2], ipaddr_l[3])

def get_ifhwaddr(ifname):
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
	except socket.error, e:
		raise ZC_Error, "Can't create socket: %s" %(e)
	ifreq = struct.pack('16sH14s', ifname, 0, '')
        SIOCGIFHWADDR = 0x8927
	try:
		try:
			ret = fcntl.ioctl(s.fileno(), SIOCGIFHWADDR, ifreq)
		finally:
			s.close()
	except IOError, e:
		raise ZC_Error, "Can't get %s hw address: %s" %(ifname, e)
	hwaddr_l = struct.unpack('16sH14B', ret)[2:8]
	return "%02X:%02X:%02X:%02X:%02X:%02X" %(hwaddr_l[0], hwaddr_l[1], hwaddr_l[2], hwaddr_l[3], hwaddr_l[4], hwaddr_l[5])

def get_loadavg(log, *args):
	filename = "/proc/loadavg"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	lstat = lines[0].split()
	status = {}
	status['1'] = float(lstat[0])
	status['5'] = float(lstat[1])
	status['15'] = float(lstat[2])

	process = lstat[3].split("/")
	status['running'] = int(process[0])
	status['total'] = int(process[1])
	status['last'] = int(lstat[4])
	num_cpus = os.sysconf('SC_NPROCESSORS_ONLN')
	status['percpu.1'] = float(lstat[0])/float(num_cpus)
	status['percpu.5'] = float(lstat[1])/float(num_cpus)
	status['percpu.15'] = float(lstat[2])/float(num_cpus)
	return status

def get_stat(log, *args):
	global cpushared, cpulock
	filename = "/proc/stat"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	cpu_fields = ['user', 'nice', 'sys', 'idle', 'iowait', 'irq', 'softirq', 'steal', 'guest']
	status = {}
	status["cpus"] = 0
	cpustat = []
	for line in lines:
		lstat = line.split()
		if (lstat[0] == "cpu"):
			for x in lstat[1:]:
				cpustat.append(int(x))
			if len(lstat) < 10:
				cpustat += [0] * (10 - len(lstat))
		elif(lstat[0][0:-1] == "cpu"):
			status["cpus"] += 1 
		else:
			status[lstat[0]] = int(lstat[1])
	if len(args) == 0 or 'cpu' in args:
		cpulock.lock(cpulock.LOCK_EX)
		cpushared.seek(0)
		tmp = cpushared.read(SYSTEM_CPU_SIZE)
		tmp = struct.unpack('%dq' %(10), tmp)
		if tmp[0]:
			i = 0
			total = 0
			deltas = []
			while i < len(cpustat):
				delta = cpustat[i]-tmp[i+1]
				deltas.append(delta)
				total += delta
				i += 1
			i = 0
			for field in cpu_fields:
				if total > 0:
					status["cpu.%s" %(field)] = float(deltas[i])/total*100
				else:
					status["cpu.%s" %(field)] = 0.0
				i += 1
		tmp = struct.pack('%dq' %(10), *([1] + cpustat))
		cpushared.seek(0)
		cpushared.write(tmp)
		cpulock.unlock()
	return status

def get_meminfo(log, *args):
	filename = "/proc/meminfo"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	status = {}
	for line in lines:
		lstat = line.split()
		if len(lstat) == 3 and lstat[-1] == 'kB':
			status[lstat[0][0:-1]] = int(lstat[1]) * 1024
		else:
			status[lstat[0][0:-1]] = int(lstat[1])
	status['MemAvail'] = status['Cached'] + status['Buffers'] + status['MemFree']
	if status['SwapTotal'] > 0:
		status['pSwapFree'] = float(status['SwapFree'])/float(status['SwapTotal']) * 100
	else:
		status['pSwapFree'] = 0.0
	return status

def get_sockstat(log, *args):
	filename = "/proc/net/sockstat"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	status = {}
	for line in lines:
		lstat = line.split()
		i = 1
		while (i<len(lstat)-1):
			status["%s.%s" %(lstat[0][0:-1], lstat[i])] = int(lstat[i+1])
			i+= 2
	return status

def get_ifdev(log, *args):
	filename = "/proc/net/dev"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	if_fields = ['RX.bytes', 'RX.packets', 'RX.errs', 'RX.drop', 'Rx.fifo', 'RX.frame', 'RX.compressed', 'RX.multicast', 'TX.bytes', 'TX.packets', 'TX.errs', 'TX.drop', 'TX.fifo', 'TX.colls', 'TX.carrier', 'TX.compressed']
	status = {}
	i = 2
	while (i < len(lines)):
		t = lines[i].replace(':', ' ')
		lstat = t.split()
		ifname = lstat[0]
		j = 1
		for field in if_fields:
			status["%s[%s]" %(field, ifname)] = int(lstat[j])
			j += 1
		i += 1
	return status

def get_ifdevs(log, *args):
	filename = "/proc/net/dev"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	status = {}
	discovery = []
	i = 2
	while (i < len(lines)):
		t = lines[i].replace(':', ' ')
		ifname = t.split()[0]
		ipaddr = 'None'
		hwaddr = 'None'
		try:
			ipaddr = get_ifipaddr(ifname)
		except ZC_Error, e:
			log.debug("%s" %(e))
		try:
			hwaddr = get_ifhwaddr(ifname)
		except ZC_Error, e:
			log.error("%s" %(e))
		discovery.append({'{#IFNAME}' : ifname, '{#IPADDR}' : ipaddr, '{#HWADDR}': hwaddr})
		i += 1
	status["discovery"] = discovery
	return status


def get_netstat(log, *args):
	filename = "/proc/net/netstat"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	status = {}
	i = 0
	while i<len(lines):
		ltitle = lines[i].split()
		lstat = lines[i+1].split()
		j = 1
		while j < len(ltitle):
			status["%s.%s" %(ltitle[0][0:-1], ltitle[j])] = lstat[j]
			j += 1
		i += 2 
	return status

def get_snmp(log, *args):
	filename = "/proc/net/snmp"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	status = {}
	i = 0
	while i<len(lines):
		ltitle = lines[i].split()
		lstat = lines[i+1].split()
		j = 1
		while j < len(ltitle):
			status["%s.%s" %(ltitle[0][0:-1], ltitle[j])] = lstat[j]
			j += 1
		i += 2 
	if len(args):
		status_args = {}
		for arg in args:
			if arg == 'currestab':
				key = "Tcp.CurrEstab"
				status_args[key] =  status[key]
		return status_args
	return status
	
def get_mtab(log, *args):
	filename = "/etc/mtab"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	status = {}
	
	for line in lines:
		(dev, fs, type) = line.split()[0:3]
		if re.search(r'^proc|sysfs|devpts|binfmt_misc|fuse\.vmware-vmblock|nfs|none$', type):
			continue
		try:
			vfs = os.statvfs(fs)
		except OSError, e:
			log.error("%s" %e)
		free = vfs[statvfs.F_BAVAIL]*vfs[statvfs.F_BSIZE]
		size = vfs[statvfs.F_BLOCKS]*vfs[statvfs.F_BSIZE]
		ffree = vfs[statvfs.F_FAVAIL]
		files = vfs[statvfs.F_FILES]
		if size > 0:
			pfree = float(free)/float(size) * 100
		else:
			pfree = 0
		if files > 0:
			pffree = float(ffree)/float(files) * 100
		else:
			pffree = 0
		status["fs.size[%s]" %(fs)] = size
		status["fs.free[%s]" %(fs)] = free
		status["fs.used[%s]" %(fs)] = size - free
		status["fs.pfree[%s]" %(fs)] = pfree
		status["fs.files[%s]" %(fs)] = files
		status["fs.ffree[%s]" %(fs)] = ffree
		status["fs.pffree[%s]" %(fs)] = pffree
		status["fs.type[%s]" %(fs)] = type
	return status

def get_mtabs(log, *args):
	filename = "/etc/mtab"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	discovry = []
	for line in lines:
		(dev, fs, type) = line.split()[0:3]
		if re.search(r'^proc|sysfs|devpts|binfmt_misc|fuse\.vmware-vmblock|nfs|none$', type):
			continue
		discovry.append({'{#FS}' : fs, '{#TYPE}' : type, '{#DEVICE}' : dev})
	status = {"discovery" : discovry}
	return status

def get_partitions(log, *args):
	filename = "/proc/partitions"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	discovry = []
	i = 2
	while i < len(lines):
		(dev) = lines[i].split()[3]
		discovry.append({'{#DEVICE}' : dev})
		i += 1
	status = {"discovery" : discovry}
	return status

def get_partition(log, *args):
	filename = "/proc/partitions"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	parts = {}
	i = 2
	while i < len(lines):
		(dev) = lines[i].split()[3]
		parts[dev] = None
		i += 1
	filename = "/proc/diskstats"
	try:
		f = open(filename, "r")
		lines = f.readlines()
		f.close()
	except ioerror, e:
		log.error("can't open %s: %s" %(filename, e))
		return none
	status = {}
	for line in lines:
		lstat = line.split()
		dev = lstat[2]
		if len(lstat) == 7:
			status["reads[%s]" %(dev)] = lstat[3]
			status["rsectors[%s]" %(dev)] = lstat[4]
			status["writes[%s]" %(dev)] = lstat[5]
			status["wsectors[%s]" %(dev)] = lstat[6]
		elif len(lstat) == 14:
			status["reads[%s]" %(dev)] = lstat[3]
			status["mreads[%s]" %(dev)] = lstat[4]
			status["rsectors[%s]" %(dev)] = lstat[5]
			status["tread[%s]" %(dev)] = lstat[6]
			status["writes[%s]" %(dev)] = lstat[7]
			status["wsectors[%s]" %(dev)] = lstat[8]
			status["twrite[%s]" %(dev)] = lstat[9]
	return status

def get_process(log, *args):
	dirname = "/proc"
	status = {'total': 0, 'sleeping': 0, 'running': 0, 'zombie': 0}
	try:
		files = os.listdir(dirname)
	except OSError, e:
		log.error("Can't list dir %s: %s" %(dirname, e))
		return None
	for file in files:
		if not re.search(r'^\d+$', file):
			continue
		spath = "%s/%s/status" %(dirname, file)
		try:
			lines = get_filelines(spath, 2)
		except ZC_Error, e:
			log.debug("%s" %e)
			continue
		status['total'] += 1
		lstat = lines[1].split()
		s = lstat[1]
		if s == 'S':
			status['sleeping'] += 1
		elif s == 'R':
			status['running'] += 1
		elif s == 'Z':
			status['zombie'] += 1
		else:
			pass
	return status
			
			
	

def get_system(log, *args):
	status = {}
	status['hostname'] = socket.gethostname()
	status['localtime'] = int(time.time())
	filename = "/proc/sys/kernel/pid_max"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	if lines != None:
		status['maxproc'] = lines[0].split()[0]
	filename = "/proc/sys/fs/file-max"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	if lines != None:
		status['maxfiles'] = lines[0].split()[0]
	filename = "/proc/uptime"
	try:
		lines = get_filelines(filename)
	except ZC_Error, e:
		log.error("%s" %e)
		return None
	if lines != None:
		status['uptime'] = (lines[0].split()[0]).split(".")[0]
	return status

def ckfsum(log, filename):
	try:
		fh = open(filename, 'rb');
	except IOError, e:
		log.error("Can't open file %s: %s" %(filename, e))
		return None
	crc = 0
	try:
		try:
			while True:
				buf = fh.read(BUFSIZE)
				if len(buf) == 0:
					break
				crc = zlib.crc32(buf, crc)
		finally:
			fh.close
	except IOError, e:
		log.error("Read file %s: %s" %(filename, e))
		return None
	except zlib.error, e:
		log.error("crc32 file %s: %s" %(filename, e))
		return None
	return crc

def get_filesum(log, filename):
	status = {}
	s = ckfsum(log, filename)
	if s == None:
		return None
	status['crc32[%s]' %(filename)] = s & 0xffffffff
	return status

def get_filestat(log, filename, *args):
	status = {}
	try:
		s = os.stat(filename)
	except OSError, e:
		log.error("Can't stat %s: %s" %(filename, e))
		return None
	status['mode'] = s[stat.ST_MODE]
	status['ino'] = s[stat.ST_INO]
	status['dev'] = s[stat.ST_DEV]
	status['nlink'] = s[stat.ST_NLINK]
	status['uid'] = s[stat.ST_UID]
	status['gid'] = s[stat.ST_GID]
	status['size'] = s[stat.ST_SIZE]
	status['atime'] = s[stat.ST_ATIME]
	status['mtime'] = s[stat.ST_MTIME]
	status['ctime'] = s[stat.ST_CTIME]
	return status
	
	
class zc_system_action:
	def __init__(self, **kwargs):
		self.handler = None
		self.args = None
		self.type = 0
		self.prefix = None
		self.nargs = 0
		for key in kwargs:
			if key == 'handler':
				self.handler = kwargs[key]
			elif key == 'args':
				self.args = kwargs[key]
			elif key == 'prefix':
				self.prefix = kwargs[key]
			elif key == 'type':
				self.type = kwargs[key]
			elif key == 'nargs':
				self.nargs = kwargs[key]
			else:
				raise TypeError, "zc_system_action() got an unexpected keyword argument '%s'" %(key)
				os._exit(1)

SYSTEM_ACTIONS = {
	'loadavg' : zc_system_action(
		handler = get_loadavg,
		args = {
			'avg' : ['1', '5', '15'],
			'percpu' : ['percpu.1', 'percpu.5', 'percpu.15'],
			'process' : ['running', 'total', 'last']
		},
		prefix = 'loadavg'
	),
	'stat' : zc_system_action(
		handler = get_stat,
		args = {
			'cpu': ['cpu.user', 'cpu.nice', 'cpu.sys', 'cpu.idle', 'cpu.iowait', 'cpu.irq', 'cpu.softirq', 'cpu.steal', 'cpu.guest'],
			'intr': ['intr'],
			'ctxt': ['ctxt'],
			'btime': ['btime'],
		},
		prefix = 'stat'
	),
	'meminfo' : zc_system_action(
		handler = get_meminfo,
		args = {
			'total': ['MemTotal'],
			'free': ['MemFree'],
			'buffers': ['Buffers'],
			'cached': ['Cached'],
			'avail': ['MemAvail'],
			'swaptotal': ['SwapTotal'],
			'swapfree': ['SwapFree', 'pSwapFree'],
		},
		prefix = 'mem'
	),
	'sockstat' : zc_system_action(
		handler = get_sockstat,
		args = {
			'tw': ['TCP.tw']
		},
		prefix = 'sockstat'
	),
	'ifdev' : zc_system_action(
		handler = get_ifdev,
		args = {
			'bytes': [r'~RX\.bytes\[.*\]$', r'~TX\.bytes\[.*\]$']
		},
		prefix = 'ifdev'
	),
	'ifdevs' : zc_system_action(
		handler = get_ifdevs,
		args = {},
		prefix = 'ifdevs'
	),
	'netstat' : zc_system_action(
		handler = get_netstat,
		args = {},
		prefix = 'netstat'
	),
	'snmp' : zc_system_action(
		handler = get_snmp,
		args = {
			'currestab': ['Tcp.CurrEstab']
		},
		prefix = 'snmp'
	),
	'mtab' : zc_system_action(
		handler = get_mtab,
		args = {},
		prefix = 'mtab'
	),
	'mtabs' : zc_system_action(
		handler = get_mtabs,
		args = {},
		prefix = 'mtabs'
	),

	'partition' : zc_system_action(
		handler = get_partition,
		args = {},
		prefix = 'partition'
	),

	'partitions' : zc_system_action(
		handler = get_partitions,
		args = {},
		prefix = 'partitions'
	),
	'system' : zc_system_action(
		handler = get_system,
		args = {},
		prefix = 'base'
	),
	'process' : zc_system_action(
		handler = get_process,
		args = {},
		prefix = 'process'
	),
	'cksum' : zc_system_action(
		handler = get_filesum,
		nargs = 1,
		prefix = 'cksum'
	),
	'filestat' : zc_system_action(
		handler = get_filestat,
		prefix = 'filestat',
		nargs = 1,
		args = {
			'mode': ['mode'],
			'ino': ['ino'],
			'dev': ['dev'],
			'nlink': ['nlink'],
			'uid': ['uid'],
			'gid': ['gid'],
			'size': ['size'],
			'atime': ['atime'],
			'mtime': ['mtime'],
			'ctime': ['ctime'],
		},

	),


}

def in_list(t, d):
	for v in t:
		if len(v) > 0:
			if v[0] == '~':
				if re.search(v[1:], d):
					return True
		if v == d:
			return True
	return False
	
	
def get_status(log, **kwargs):
	status = {}	
	if 'gets' in kwargs:
		for x in kwargs['gets']:
			tmp = {}
			action = x[0]
			action_args = []
			if len(x) > 1:
				action_args = x[1:]
			if action.handler == None:
				continue
			s = action.handler(log, *action_args)
			if s == None:
				continue
			if len(action_args) == action.nargs:
				for key in s:
					status['%s.%s' %(action.prefix, key)] = s[key]
			else:
				for key in s:
					for akey in action_args[action.nargs:]:
						if action.args[akey] == None:
							continue
						if in_list(action.args[akey], key):
							status['%s.%s' %(action.prefix, key)] = s[key]
	if 'commands' in kwargs:
		for key in kwargs['commands']:
			now = time.time()
			command = kwargs['commands'][key][0]
			timeout = kwargs['command_timeout']
			if kwargs['commands'][key][1] != None:
				timeout = kwargs['commands'][key][1]
			log.debug('%s "%s" execute %s' %(kwargs['info'], key, command))
			try:
				try:
					out, err = cmd.call(command, timeout)
					if len(err):
						log.error('%s "%s" "%s"' %(kwargs['info'], key, err))
				finally:
					use_time = time.time() - now
			except cmd.error, e:
				log.error('%s "%s" %.6fs "%s"' %(kwargs['info'], key, use_time, e))
				return None
			log.info('%s "%s" %.6fs' %(kwargs['info'], key, use_time))
			status[key] = re.sub(r'\n$', '',  out)
	return status


def task_handler(log, task):
	conf = task['conf']
	mctx = conf['modules'][module.ctx_index]
	stype = conf['ctx']['type']
	now = time.time()
	kwargs = {}
	if 'system_gets' in mctx:
		kwargs['gets'] = mctx['system_gets']
	if 'system_commands' in mctx:
		kwargs['commands'] = mctx['system_commands']
	kwargs['info'] = task['task_info']
	kwargs['command_timeout'] = mctx['system_command_timeout']
	status = get_status(log, **kwargs)
	use_time = time.time() - now
	log.info('%s "localhost" %.6fs' %(task['task_info'], use_time))
	r = []
	for key in status:
		r.append({'key' : key, 'value' : status[key]})
	return r




def system_set_get_slot(cf, cmd, conf):
	key = cmd.key
	if key not in conf:
		conf[key] = []
	larg = cf.args[1].lower()
	if larg not in SYSTEM_ACTIONS:
		cf.log.error("Unsupport name '%s' in %s at line %d: %d" %(cf.args[1], cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
		return ZC_ERROR
	
	action = SYSTEM_ACTIONS[larg]
	args = [action]
	nargs = action.nargs
	if nargs < 0:
		nargs = 0
	if nargs > 0:
		if len(cf.args) < 2+nargs:
			cf.log.error("Param '%s': too less arguments in %s at line %d: %d" %(cf.args[1], cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
			return ZC_ERROR
		args += cf.args[2:nargs+2]
	if len(cf.args) > nargs+2:
		params = cf.args[nargs+2:]
		for param in params:
			lparam = param.lower()
			if action.args == None or (isinstance(action.args, dict) and lparam not in action.args):
				cf.log.error("Unsupport param '%s' in %s at line %d: %d" %(param, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
				return ZC_ERROR
			args.append(lparam)
	conf[key].append(args)
	return ZC_OK

def system_set_command_slot(cf, cmd, conf):
	if cmd.key == None:
		cf.log.error("command '%s' is not defined in %s at line %d: %d" %(cf.args[1], cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
		return ZC_ERROR
	timeout = None
	if len(cf.args) == 4:
		try:
			var = zc_parse_sec(cf.args[3])
		except ValueError, e:
			cf.log.error("Can't parse '%s' %s in %s at line %d: %d" %(cf.args[3], e, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
			return ZC_ERROR
		timeout = var

	if cmd.key not in conf:
		conf[cmd.key] = {} 
	conf[cmd.key][cf.args[1]]=[cf.args[2], timeout]
	return ZC_OK



commands = [
	zc_command(
		name = 'system_get',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_1MORE|ZC_CONF_MULTI,
		set = system_set_get_slot,
		key = 'system_gets',
		describe = 'System get attribe'
	),
	zc_command(
		name = 'system_command',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE23|ZC_CONF_MULTI,
		set = system_set_command_slot,
		key = 'system_commands',
		describe = 'System get attribe by command'
	),
	zc_command(
		name = 'system_command_timeout',
		type = ZC_CHECKER_MAIN_CONF|ZC_CHECKER_GROUP_CONF|ZC_CHECKER_HOST_CONF|ZC_CHECKER_SVC_CONF|ZC_CONF_TAKE1,
		set = zc_conf_set_sec_slot,
		key = 'system_command_timeout',
		describe = 'Command timeout'
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
	ctx = mc_ctx,
	init_master = init_master,
	)
