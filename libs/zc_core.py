"""\
Core module
Copyright (C) Zhou Changrong
"""
import os, sys, getopt, logging, socket, struct, signal, time, select, errno, ctypes, random, fcntl, pwd, grp
from setproctitle import setproctitle,getproctitle
from zc_common import *
import heap
from zc_config_h import *
from zc_core_h import *
import zc_config

ZC_DEF_CONFIG_FILE      = '/etc/zc.conf'
ZC_DEF_MAXWORKERS       = 20
ZC_DEF_STARTWORKERS     = 5
ZC_DEF_MINSPAREWORKERS  = 5
ZC_DEF_MAXSPAREWORKERS  = 25
ZC_DEF_LOGFILE = 'logs/zc.log'
ZC_DEF_LOGLEVEL = logging.INFO
#ZC_DEF_LOGLEVEL = logging.DEBUG
ZC_DEF_PIDFILE = 'logs/zc.pid'
action = None

progname = 'zc'
tasks = None
quene = None
script_dir = None
processes = None
processeshash = None
old_processes = None
old_processes_hash = None
num_processes = 0
spare_processes = None
daemon = False
shutdown = False
debug = False
reconfigure = False
reopen = False
poll = None
poll_type = 'epoll'
config_file = ZC_DEF_CONFIG_FILE
#logfh = None
#loghandler = None
cwd = os.getcwd()

ZC_BASEDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
module_dir = ZC_BASEDIR + '/modules'
sys.path.insert(0, module_dir)

def zc_master_signal(a, b):
	global shutdown,reconfigure, reopen
	if a in (signal.SIGTERM, signal.SIGINT):
		if not shutdown:
			os.kill(0, signal.SIGTERM)
		shutdown = True
	elif a == signal.SIGHUP:
		if not reconfigure:
			os.kill(0, signal.SIGHUP)
		reconfigure = True
	elif a == signal.SIGUSR1:
		if not reopen:
			os.kill(0, signal.SIGUSR1)
		reopen = True

def zc_worker_signal(a, b):
	global shutdown,reconfigure
	if a in (signal.SIGTERM, signal.SIGINT):
		shutdown = True
	elif a == signal.SIGHUP:
		reconfigure = True
	elif a == signal.SIGUSR1:
		reopen = True

def register_module(cf, m):
	m.module.ctx_index = len(cf.modules)
	cf.conf['modules'].append(None)
	cf.modules.append(m.module)
	if m.module.commands == None:
		return ZC_OK
	for cmd in m.module.commands:
		t = cf.commands.get(cmd.name)
		if t != None:
			cf.log.error("Command '%s' is already defined in %s module" %(cmd.name,  cf.modules[t.ctx_index].name))
			return ZC_ERROR
		cmd.ctx_index = m.module.ctx_index
		cf.commands[cmd.name] = cmd
	return ZC_OK
	
def load_module(cf, name):
	cf.log.debug("Load module mod_%s" %(name))
	try:
		exec("import mod_%s as m\n" %(name))
	except ImportError, e:
		cf.log.error("Can't load module %s: %s" %(name, e))
		return ZC_ERROR
	if name != m.module.name:
		sys.modules.pop("mod_%s" %(name))
		cf.log.error("Module name is not match(\"%s\" <=> \"%s\"), can't load" %(name, m.module.name))
		return ZC_ERROR
	if register_module(cf, m) == ZC_ERROR:
		return ZC_ERROR
	return ZC_OK
def unload_module(cf, name):
	cf.log.info("Unload module mod_%s" %(name))
	del sys.modules["mod_%s" %(name)]
	return ZC_OK

def load_module_slot(cf, cmd, conf):
	for name in cf.args[1:]:
		if load_module(cf, name) == ZC_ERROR:
			return ZC_ERROR
	return ZC_OK

def zc_conf_set_use_slot(cf, cmd, conf):
	if zc_conf_check_slot(cf, cmd, conf) != ZC_OK:
		return ZC_ERROR
	if cf.args[1] not in ('epoll', 'poll'):
		cf.log.error("Unsupport poll type %s" %(cf.args[1]))
		return ZC_ERROR
	conf[cmd.key] = cf.args[1]
	return ZC_OK


commands = [
	zc_command(
		name = 'pidfile',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		describe = 'Set pid file path',
		key  = 'pidfile'
		),
	zc_command(
		name = 'logfile',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		describe = 'Set log file path',
		key  = 'logfile'
		),
	zc_command(
		name = 'loglevel',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_loglevel_slot,
		describe = 'Set log level',
		key  = 'loglevel'
		),
	zc_command(
		name = 'loadmodule',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_1MORE|ZC_CONF_MULTI,
		set = load_module_slot,
		describe = 'Load a module',
		key = None
		),
	zc_command(
		name = 'user',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		describe = 'Worker user',
		key = 'user',
		),
	zc_command(
		name = 'group',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_str_slot,
		describe = 'Worker group',
		key = 'group',
		),
	zc_command(
		name = 'maxworkers',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_int_slot,
		describe = 'Max workers',
		key = 'maxworkers',
		),
	zc_command(
		name = 'maxspareworkers',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_int_slot,
		describe = 'Max spare workers',
		key = 'maxspareworkers',
		),
	zc_command(
		name = 'minspareworkers',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_int_slot,
		describe = 'Min spare workers',
		key = 'minspareworkers',
		),
	zc_command(
		name = 'startworkers',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_int_slot,
		describe = 'Max start check workers',
		key = 'startworkers',
		),
	zc_command(
		name = 'use',
		type = ZC_MAIN_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_set_use_slot,
		describe = 'Use poll or epoll',
		key = 'use',
		),

]

module = zc_module(
	name = 'core',
	version = '0.1.0',
	type = ZC_CORE_MODULE,
	commands = commands,
	ctx = None
	)

def usage():
	print "Usage: %s [OPTIONS...] <start|stop|reload|reopen|restart>" %(sys.argv[0])
	print
	print "Options:"
	print " -c/--config-file <path> Config file"
	print " -d/--debug         Show debug message"
	print " -D/--daemon        Daemon"
	print " -h/--help          Display this page and exit"
	print " -v/--version       Display version and exit"

def process_size():
	return struct.calcsize('iii')

def pack_process(process):
	s = struct.pack('iii', process['id'], process['pid'], process['task_id'])
	return s

def unpack_process(s):
	process = {}
	process['id'], process['pid'], process['task_id'] = struct.unpack('iii', s)
	return process

def process_cycle(log, process):
	global tasks, reconfigure, reload, shutdown
	p = process['channel']
	while True:
		setproctitle('%s: worker process %5d (spare)' %(progname, process['id']))
		if reconfigure or reopen:
			os._exit(0)
		if shutdown:
			os._exit(0)
		try:
			os.write(p[1].fileno(), pack_process(process))
			length = process_size()
			buf = os.read(p[1].fileno(), length)
			setproctitle('%s: worker process %5d (busy)' %(progname, process['id']))
		except OSError, e:
			if shutdown:
				os._exit(0)
			os._exit(1)
		if shutdown:
			os._exit(0)
		tmp = unpack_process(buf)
		process['task_id'] = tmp['task_id']
		task = tasks[process['task_id']]
		task['process_task'](log, task)

def cmp_handler(a, b):
	return cmp(a['next_time'], b['next_time'])

def set_worker_signal():
	signal.signal(signal.SIGINT, zc_worker_signal)
	signal.signal(signal.SIGTERM, zc_worker_signal)
	signal.signal(signal.SIGPIPE, zc_worker_signal)
	signal.signal(signal.SIGHUP, zc_worker_signal)
	signal.signal(signal.SIGUSR1, zc_worker_signal)




def start_worker_process(cf, id=-1):
	global tasks, processes, processes_hash, num_processes, poll
	ctx = cf.conf['ctx']
	if id < 0:
		s = 0
		while s < ctx['maxworkers']:
			if processes[s]['pid'] == -1:
				break
			s+=1
	else:
		s = id
	pid = os.getpid()
	p = socket.socketpair()
	pfd = [p[0].fileno(), p[1].fileno()]
	flags = fcntl.fcntl(pfd[0], fcntl.F_GETFD)
	fcntl.fcntl(pfd[0], fcntl.F_SETFD, flags|fcntl.FD_CLOEXEC)
	flags = fcntl.fcntl(pfd[1], fcntl.F_GETFD)
	fcntl.fcntl(pfd[1], fcntl.F_SETFD, flags|fcntl.FD_CLOEXEC)
	fcntl.fcntl(pfd[1], fcntl.F_SETOWN, pid)
	processes[s]['id'] = s
	processes[s]['channel'] = p
	try:
		pid = os.fork()
	except OSError, e:
		cf.log.error("Can't fork")
		return None
	if pid == 0:
		pid = os.getpid()
		processes[s]['pid'] = pid
		processes[s]['task_id'] = -1
		p[0].close()
		euid = os.geteuid()
		if euid == 0:
			if ctx['group'] != None:
				os.setgid(ctx['group'])
			if ctx['user'] != None:
				os.setuid(ctx['user'])

		set_worker_signal()
		setproctitle('%s: worker process %5d (prepare)' %(progname, s))
		process_cycle(cf.log, processes[s])
		os._exit(0)
	else:
		processes[s]['pid'] = pid
		processes_hash[pid] = s
		processes[s]['task_id'] = -1
		p[1].close()
		processes[s]['status'] = ZC_PREPARE
		num_processes += 1
	return processes[s]


def wait_process(cf):
	global processes,num_processes, processes_hash, spare_processes, old_processes_hash, shutdown, reconfigure, tasks, quene
	try:
		pid, status = os.waitpid(0, os.WNOHANG)
	except OSError, e:
		return -1
	if pid == 0:
		return 0
	cf.log.info('Pid %d exit with status %d' %(pid, status))
	if old_processes_hash != None and pid in old_processes_hash:
		# free process data and close channel
		del old_processes_hash[pid]
		if len(old_processes_hash) == 0:
			old_processes_hash = None
		return pid
	num_processes -= 1
	id = processes_hash[pid]
	process = processes[id]
	if process['status'] == ZC_SPARE:
		spare_processes.remove(process)
	elif process['status'] == ZC_BUSY:
		# epoll is not need unregister fd, cause it to be removed from all epoll sets automatically
		poll.unregister(process['channel'][0].fileno())
		now = time.time()
		node = quene.node()
		node.data = {'next_time': now + tasks[process['task_id']]['interval'], 'task_id': process['task_id']}
		quene.insert(node)
	# close and free channel
	processes[id]['channel'] = None
	processes[id]['pid'] = -1
	del processes_hash[pid]
	return pid
	
	
	

def create_daemon():
	pid = os.fork()
	if pid == -1:
		os._exit(1)
	if pid > 0:
		os._exit(0)
	os.setsid()

def start_worker_processes(cf):
	global processes, spare_processes, tasks
	ctx = cf.conf['ctx']
	startworkers = ctx['startworkers']
	id = 0
	while id < startworkers: 
		process = start_worker_process(cf, id)
		if process == None:
			break
		process['status'] = ZC_SPARE
		spare_processes.append(process)
		id+=1

def set_master_signal():
	signal.signal(signal.SIGINT, zc_master_signal)
	signal.signal(signal.SIGTERM, zc_master_signal)
	signal.signal(signal.SIGPIPE, zc_master_signal)
	signal.signal(signal.SIGHUP, zc_master_signal)
	signal.signal(signal.SIGUSR1, zc_master_signal)

def set_master_signal2():
	signal.signal(signal.SIGINT, signal.SIG_IGN)
	signal.signal(signal.SIGTERM, signal.SIG_IGN)
	signal.signal(signal.SIGPIPE, signal.SIG_IGN)
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
	signal.signal(signal.SIGUSR1, signal.SIG_IGN)


def event_cycle(cf):
	global reconfigure, shutdown, reopen, poll, poll_type, spare_processes, processes, num_processes, processes_hash, old_processes_hash, quene
	ctx = cf.conf['ctx']
	quene = heap.heap(128, cmp_handler)
	i = 0
	spare_processes = []
	processes = []
	processes_hash = {}
	num_processes = 0
	now = time.time()
	for t in tasks:
		node = quene.node()
		node.data = {'next_time': now + random.uniform(0, t['interval']), 'task_id': i}
		quene.insert(node)
		i += 1
	s = 0
	while s < ctx['maxworkers']:
		processes.append({'id' : -1, 'pid' : -1, 'channel' : None, 'task_id' : -1, 'status' : ZC_PREPARE})
		s+=1

	set_master_signal()
	start_worker_processes(cf)
	rc = None
	increase_times = 0
	change_time = time.time()
	decrease_times = 0
	last_count_time = time.time()
	do_tasks = 0
	while True:
		wait_process(cf)
		if shutdown:
			if num_processes == 0:
				break
			time.sleep(0.001)
			continue
		if reconfigure or reopen:
			old_processes_hash = processes_hash
			set_master_signal2()
			break
		now = time.time()
		s = quene.top()
		while s != None and s.data['next_time'] <= now and len(spare_processes) > 0:
			quene.delete(s)
			process = spare_processes.pop()
			process['task_id'] = s.data['task_id']
			fd = process['channel'][0].fileno()
			delay = now - s.data['next_time']
			if delay > 1:
				cf.log.warn('start delay %f, quene busy' %(delay))
			cf.log.debug("Send process info to fd %d" %(fd))
			try:
				fd_send(fd, pack_process(process))
			except ZC_Error, e:
				# SIG_PIPE
				cf.log.error("Send process info to %d: %s" %(fd, e))
				process['status'] = ZC_PREPARE
				continue
			cf.log.debug("poll regiser %d" %(fd))
			tasks[process['task_id']]['last_time'] = s.data['next_time']
			poll.register(fd, select.EPOLLIN|select.EPOLLET)
			process['status'] = ZC_BUSY
			s = quene.top()
		timeout = 0.5
		if (len(spare_processes) > 0):
			s = quene.top()
			if s != None:
				now = time.time()
				delta = s.data['next_time'] - now
				if delta < 0:
					delta = 0
				if delta < timeout:
					timeout = delta
		if poll_type == 'epoll':
			try:
				rc = poll.poll(timeout)
			except IOError, e:
				if e.errno == errno.EINTR:
					continue
				cf.log.error("poll %d %s" %(e.errno, e.strerror))
				shutdown = True
				continue
		else:

			timeout *= 1000
			try:
				rc = poll.poll(timeout)
			except select.error, e:
				if e[0] == errno.EINTR:
					continue
				cf.log.error("poll %d %s" %(e.errno, e.strerror))
				shutdown = True
				continue
		num_spare_processes = len(spare_processes)
		if (num_processes > ctx['startworkers']) and (num_processes >= ctx['startworkers']) and (num_spare_processes > ctx['maxspareworkers']):
			increase_times = 0
			now = time.time()
			if change_time <= now-1:
				max_decreases = num_spare_processes-ctx['maxspareworkers']
				t = num_processes-ctx['startworkers']
				if max_decreases > t:
					max_decreases = t
				if max_decreases > 0:
					i = 0
					while True:
						if i >= max_decrease:
							break
						x = decrease_times
						if x >= 4:
							x = 4
						if i >= 2**x:
							break
						process = spare_processes.pop()
						process['status'] = ZC_PREPARE
						cf.log.debug("kill pid %d(SIGTTERM)" %(process['pid']))
						os.kill(process['pid'], signal.SIGTERM)
						i += 1
					if i>=max_decreases:
						decrease_times = 0
					change_time = time.time()
					if decrease_times < 4:
						decrease_times += 1
		elif (num_processes < ctx['startworkers']) or (num_processes < ctx['maxworkers']) and (num_spare_processes < ctx['minspareworkers']):
			decrease_times = 0
			now = time.time()
			if change_time <= now-1:
				t = ctx['startworkers'] - num_processes
				if t > 0:
					max_increases = t
				else:
					max_increases = 0
				t = ctx['minspareworkers'] - num_spare_processes
				if max_increases<t:
					max_increases = t
				t = ctx['maxworkers'] - num_processes
				if max_increases > t:
					max_increases = t
				if max_increases > 0:
					i = 0
					while True:
						if i >= max_increase:
							break
						x = increase_times
						if x >= 4:
							x = 4
						if i >= 2**x:
							break
						process = start_worker_process(cf)
						process['status'] = ZC_SPARE
						spare_processes.append(process)
						i += 1
					if i>=max_increases:
						increase_times = 0
					change_time = time.time()
					increase_times += 1
		else:
			increase_times = 0
			decrease_times = 0
		if cf.log.getEffectiveLevel() <= logging.INFO:		
			now = time.time()
			if (now >= last_count_time+5):
				cf.log.info("Do tasks %f/sec" %(float(do_tasks)/5))
				last_count_time = now
				do_tasks = 0
		if rc == None:
			continue
		if shutdown or reconfigure:
			continue
		for fd, status in rc:
			if shutdown or reconfigure:
				break
			if status  & select.POLLIN:
				cf.log.debug("Recv process info from fd %d" %(fd))
				try:
					r_len = process_size()
					s = fd_recv(fd, r_len)
					if len(s) != r_len:
						cf.log.debug('fd %d was client abort' %(fd))
						continue
				except ZC_Error, e:
					# No error here
					cf.log.error("Recv process info from fd: %s" %(e))
					continue
				process = unpack_process(s)
				process = processes[process['id']]
				now = time.time()
				node = quene.node()
				last_time = tasks[process['task_id']]['last_time']
				interval =  tasks[process['task_id']]['interval']
				next_time = now
				delta = interval-(now-last_time)
				if delta > 0:
					next_time += delta
				elif delta < 0:
					cf.log.warn('%s %s "task is too slow or service interval is too small(%f)"' %(str(tasks[process['task_id']]['task_info']), now-last_time, delta))
				node.data = {'next_time': next_time, 'task_id': process['task_id']}
				quene.insert(node)
				do_tasks += 1
				cf.log.debug("poll unregiser %d" %(fd))
				poll.unregister(fd)
				process['status'] = ZC_SPARE
				spare_processes.append(process)
			if shutdown or reconfigure or reopen:
				break
		else:
			continue
		
	return



def parse_args():
	global config_file, daemon, debug, action
	try:  
		opts,args = getopt.gnu_getopt(sys.argv[1:], "c:dDhv", ["config-file=", "debug", "daemon", "help", "version"])
		for opt,arg in opts:
			if opt in ("-c", "--config-file"):
				config_file = os.path.abspath(arg)
			elif opt in ("-h", "--help"):
				usage()
				os._exit(0)
			elif opt in ("-D", "--daemon"):
				daemon = True
			elif opt in ("-d", "--debug"):
				debug = True
			elif opt in ("-v", "--version"):
				print ZC_VERSION
				os._exit(0)
	except getopt.GetoptError, e:
		sys.stderr.write("Error: %s\n" %(e))
		os._exit(1)
	if len(args) > 1:
		sys.stderr.write("Error: Too many arguments\n")	
		usage()
		os._exit(1)
	elif len(args) < 1:
		sys.stderr.write("Error: Too few arguemnts\n")
		usage()
		os._exit(1)
	if args[0] not in ('start', 'stop', 'reload', 'restart', 'check'):
		sys.stderr.write("Unkown action: '%s'\n" %(args[0]))
		usage()
		os._exit(1)
	action = args[0]

def master_process():
	global reconfigure, reopen, tasks, poll, poll_type, action
	logfh = None
	oldlogfh = None
	loghandler = None
	rc = 0
	log = None
	cf = None
	pf = None
	name = getproctitle()
	while True:
		if reconfigure:
			log = cf.log
			for m in cf.modules:
				if m.name not in ('config', 'core'):
					unload_module(cf, m.name) 
		if not reopen:
			tasks = []
			cf = zc_conf_file()
			stderr_handler = None
			if reconfigure:
				cf.log = log
			else:
				cf.log = logging.getLogger()
				cf.log.setLevel(ZC_DEF_LOGLEVEL)
				stderr_handler=logging.StreamHandler(sys.stderr)
				formatter = logging.Formatter("%(levelname)s:  %(message)s")
				stderr_handler.setFormatter(formatter)
				cf.log.addHandler(stderr_handler)
			cf.conf = {
				'modules' : [],
				'ctx' : {
					'defines' : {},
					},
				}
			cf.main_conf = cf.conf
			mod = sys.modules[__name__]
			register_module(cf, mod)
			register_module(cf, zc_config)
			sys.path.insert(0, module_dir)
			if cf.file_parser(config_file) == ZC_ERROR:
				if action == 'check':
					cf.log.error('Check failed')
				os._exit(1)
			if action == 'check':
				cf.log.info('Check ok')
				os._exit(0)
			ctx = cf.conf['ctx']
			ctx['maxworkers'] = zc_dict_get_ge(ctx, 'maxworkers', 0, ZC_DEF_MAXWORKERS)
			ctx['startworkers'] = zc_dict_get_ge(ctx, 'startworkers', 0, ZC_DEF_STARTWORKERS)
			ctx['minspareworkers'] = zc_dict_get_ge(ctx, 'minspareworkers', 0, ZC_DEF_MINSPAREWORKERS)
			ctx['maxspareworkers'] = zc_dict_get_ge(ctx, 'maxspareworkers', 0, ZC_DEF_MAXSPAREWORKERS)
			ctx['logfile'] = ctx.get('logfile', ZC_DEF_LOGFILE)
			ctx['loglevel'] = ctx.get('loglevel', ZC_DEF_LOGLEVEL)
			ctx['user'] = ctx.get('user')
			ctx['group'] = ctx.get('group')
			ctx['pidfile'] = ctx.get('pidfile', ZC_DEF_PIDFILE)
			if ctx['user'] != None:
				pw = pwd.getpwnam(ctx['user'])
				ctx['user'] = pw.pw_uid
			if ctx['group'] != None:
				gr = grp.getgrnam(ctx['group'])
				ctx['group'] = gr.gr_gid
			if ctx['maxworkers'] < 1:
				cf.log.error("Number maxwokers can't less than 1")
				os._exit(1)
			if ctx['minspareworkers'] < 0 or ctx['maxspareworkers'] < 0:
				cf.log.error("Number minspareworkers or maxspareworkers can't less than 0")
				os._exit(1)
				
			if ctx['startworkers'] > ctx['maxworkers']:
				cf.log.warn("Startworkers %d is great than maxworkers %d, change startworkers to %d" %(ctx['startworkers'], ctx['maxworkers'], ctx['maxworkers']))
				ctx['startworkers'] = ctx['maxworkers']
			if ctx['minspareworkers'] > ctx['maxspareworkers']:
				cf.log.warn("Minspareworkers %d is great than maxspareworkers %d, change minspareworkers to %d" %(ctx['minspareworkers'], ctx['maxspareworkers'], ctx['maxspareworkers']))
				ctx['minspareworkers'] = ctx['maxspareworkers']
			pf = pid_file(ctx['pidfile'])
			if pf.is_exists():
				pids = pf.read()
				if is_running(pids):
					if action == 'start':
						cf.log.error("%s (pid %s ) is running" %(progname, " ".join(str(pid) for pid in pids)))
						os._exit(1)
					elif action == 'reload':
						kill_pids(pids, signal.SIGHUP)
						os._exit(0)
					elif action == 'reopen':
						kill_pids(pids, signal.SIGUSR1)
						os._exit(0)
					elif action in ('stop', 'restart'):
						kill_pids(pids, signal.SIGTERM)
						while True:
							time.sleep(0.01)
							if not is_running(pids):
								break
						if action == 'stop':
							os._exit(0)
				else:
					if action == 'stop':
						cf.log.error("is not running, but pid file is exists")
						os._exit(1)
					elif action == 'reload' or action == 'reopen':
						cf.log.error("is not running, but pid file is exists")
						os._exit(1)
			else:
				if action in ('stop', 'restart'):
					cf.log.error("is not running, is already stoped")
				if action == 'stop':
					os._exit(1)
					
				
			action = None
			if not reconfigure and daemon:
				create_daemon()
			pf.write(["%d" %(os.getpid())])
		oldlogfh = logfh
		try:
			logfh = open(ctx['logfile'], 'a')
		except IOError, e:
			cf.log.error("Can't open %s: %s" %(ctx['logfile'], e))
			os._exit(1)
		if not reconfigure and not debug:
			cf.log.removeHandler(stderr_handler)
			stderr_handler.close()
		oldloghandler = loghandler
		loghandler=logging.StreamHandler(logfh)
		formatter = logging.Formatter("%(asctime)s - %(levelname)s - " + progname + "[%(process)d]:  %(message)s")
		loghandler.setFormatter(formatter)
		cf.log.addHandler(loghandler)
		cf.log.setLevel(ctx['loglevel'])
		if oldlogfh == None:
			sys.stdout.close()
			sys.stderr.close()
		else:
			cf.log.removeHandler(oldloghandler)
			oldloghandler.close()
			oldlogfh.close()
			oldloghandler = None
			oldlogfh = None
		sys.stdout = logfh
		sys.stderr = logfh
		if not reopen:
			for m in cf.modules:
				if m.type != ZC_CORE_MODULE:
					continue
				if m.init_master == None:
					continue
				if m.init_master(cf, cf.conf['modules'][m.ctx_index], tasks) == ZC_ERROR:
					os._exit(1)
		if poll != None:
			poll.close()
		if 'use' in ctx and ctx['use'] == "epoll":
			if hasattr(select, 'epoll'):
				poll_type = 'epoll'
			else:
				cf.log.warn("Python module select is not support epoll, use poll instead");
				poll_type = 'poll'
		else:
			poll_type = 'poll'
		if poll_type == 'epoll':
			poll = select.epoll()
		else:
			poll = select.poll()
		if not reopen:
			setproctitle('%s: master process %s' %(progname, name))
		if reconfigure:
			reconfigure = False
		if reopen:
			reopen = False
		event_cycle(cf)
		if shutdown:
			cf.log.info("shutdown")
			break
		if reconfigure:
			cf.log.info("reload config file")
			continue
		if reopen:
			cf.log.info("reopen log file")
			continue
	loghandler.close()
	logging.shutdown()
	logfh.close()
	pf.remove()
	os._exit(rc)


def run():
	parse_args()
	os.chdir(ZC_BASEDIR)
	master_process()
	os.chdir(cwd)
