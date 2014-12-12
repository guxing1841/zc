import os, fcntl, errno
class error(EnvironmentError):
	def __init__(self, message):
		self.message = message
	def __str__(self):
		return str(self.message)
#class sared:
#	def __init__(self):
#		self.shared = None
#		self.fd = -1
#	def malloc(self,  size):
#		try:
#			self.shared = mmap.mmap(-1, size, mmap.MAP_SHARED, mmap.PROT_WRITE)
#		except mmap.error, e:
#			raise error, e
#	def free(self):
#		self.shared.close()
#	def __del__(self):
#		self.close()

class locked:
	LOCK_SH = fcntl.LOCK_SH
	LOCK_EX = fcntl.LOCK_EX
	LOCK_NB = fcntl.LOCK_NB
	def __init__(self, file, size = 0):
		try:
			f = open(file, 'w')
			if size > 0:
				f.write('0' * size)
		except IOError, e:
			raise error, e
		try:
			os.unlink(file)
		except OSError, e:
			raise error, e
		self.f = f
		self.file = file
	def lock(self, flag, lenght = 0, start = 0, whence = 0):
		try:
			fcntl.lockf(self.f, flag, lenght, start, whence)
		except IOError, e:
			if e.errno == errno.EAGAIN:
				return False
			else:
				raise error, e
		return True
	def unlock(self, lenght = 0, start = 0, whence = 0):
		try:
			fcntl.lockf(self.f, fcntl.LOCK_UN, lenght, start, whence)
		except IOError, e:
			raise error, e
		return True
	def __del__(self):
		self.file = None
		self.f.close()
		self.f = None

#import time
#a = locked('zc.lock')
#pid = os.fork()
#if pid == 0:
#	print a.lock(a.LOCK_EX|a.LOCK_NB)
#	time.sleep(10)
#else:
#	time.sleep(1)
#	print a.lock(a.LOCK_EX|a.LOCK_NB)
#	print 'here'
#a.unlock()
