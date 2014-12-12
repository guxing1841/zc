import os, datetime

class error(EnvironmentError):
	def __init__(self, message):
		self.message = message
	def __str__(self):
		return str(self.message)

ALL=0
DEBUG=1
INFO=2
WARNING=3
ERROR=4
CRITICAL=5

LEVELNAMES = {
	ALL        : 'ALL',
	DEBUG      : 'DEBUG',
	INFO       : 'INFO',
	WARNING    : 'WARNING',
	ERROR      : 'ERROR',
	CRITICAL   : 'CRITICAL',
}


def asctime(**kwargs):
	now = datetime.datetime.now()
	return now.strftime("%Y-%m-%d %H:%M:%S") + (".%06d" %(now.microsecond))
def getpid(**kwargs):
	return os.getpid()
def levelname(**kwargs):
	level = kwargs['level']
	if level in LEVELNAMES:
		return LEVELNAMES[level]
	return '-'
def message(**kwargs):
	return kwargs['message']

FORMAT_ACTIONS = {
	'asctime': asctime,
	'process': getpid,
	'levelname': levelname,
	'message': message,
}

class Handler:
	file = None
	stream = None
	format = None
	def __init__(self, **kwargs):
		"""__init__(file=None, stream=None, format=None)"""
		for key in kwargs:
			if key == 'file':
				self.file = kwargs[key]
			elif key == 'stream':
				self.stream = kwargs[key]
			elif key == 'format':
				self.setFormatter(kwargs[key])
			else:
				raise error, "unkown keyword '%s'" %(key)
	def open(self, uid=-1, gid=-1):
		if self.stream == None and self.file == None:
			raise error, "stream and file can't both None"
		if self.file != None:
			oldstream = self.stream
			try:
				h = open(self.file, "a")
			except IOError, e:
				raise error, "Can't open %s: %s" %(self.file, e)
			self.stream = h
			if oldstream != None:
				oldstream.close()
			if uid >= 0 or gid >= 0:
				try:
					os.chown(self.file, uid, gid)
				except OSError, e:
					raise error, "Can't chown %s to uid(%d) gid(%d): %s" %(self.file, uid, gid, e)

	def setFormatter(self, format):
		"""setFormatter(format)"""
		if format == None:
			self.format = None
			return
		fmt = ['', []]
		i = 0
		p = False
		in_quote = False
		keyword = None
		while i<len(format):
			c = format[i]
			if in_quote:
				if c == ')':
					if len(keyword) == 0:
						raise error, 'empty'
					if keyword not in FORMAT_ACTIONS:
						raise error, 'invalid %s' %keyword
					fmt[1].append(FORMAT_ACTIONS[keyword])
					in_quote = False
				else:
					if c not in 'abcdefghijklmnopqrstuvwxyz':
						raise error, 'char %s is invalid' %c
					keyword += c
			elif not p and c == '%':
				p = True
				fmt[0] += c
			elif c == '%':
				p = False
				fmt[0] += c
			elif p and c == '(':
				keyword = ""
				in_quote = True
				p = False
			else:
				p = False
				fmt[0] += c
			i+=1
		self.format = fmt
	def close(self):
		self.stream = None
		return True
class logging:

	def __init__(self):
		self.handlers = []
		self.level = INFO
	def addHandler(self, handler, uid = -1, gid = -1):
		handler.open(uid, gid)
		self.handlers.append(handler)
		return True
	def removeHandler(self, handler):
		return self.handlers.remove(handler)
	def setLevel(self, level):
		self.level = level
		return True
	def reopen(self, user=None):
		for handler in self.handlers:
			handler.open(user)
	def log(self, level, msg):
		if level > 0 and level < self.level:
			return False
		for handler in self.handlers:
			format = handler.format
			stream = handler.stream
			if format == None:
				stream.write("%s\n" %(msg))
				continue
			args = []
			for a in format[1]:
				args.append(a(level=level, message=msg))
			stream.write((format[0]+"\n") %tuple(args))
			stream.flush()
		return True
	def all(self, msg):
		"""all(msg) -> bool"""
		return self.log(ALL, msg)
	def debug(self, msg):
		"""debug(msg) -> bool"""
		return self.log(DEBUG, msg)
	def info(self, msg):
		"""info(msg) -> bool"""
		return self.log(INFO, msg)
	def warn(self, msg):
		"""warn(msg) -> bool"""
		return self.log(WARNING, msg)
	def warning(msg):
		"""warning(msg) -> bool"""
		return self.log(WARNING, msg)
	def error(self, msg):
		"""error(msg) -> bool"""
		return self.log(ERROR, msg)
	def critical(self, msg):
		"""cirtical(msg) -> bool"""
		return self.log(CRITICAL, msg)
	def getEffectiveLevel(self):
		"""getEffectiveLevel() -> int"""
		return self.level

def addLevelName(level, name):
	"""addLevelName(level, name)"""
	global LEVELNAMES
	LEVELNAMES[level] = name
