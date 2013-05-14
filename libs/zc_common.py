"""\
Common
Copyright (C) Zhou Changrong
"""

import os, socket

class ZC_Error(EnvironmentError):
	def __init__(self, message):
		self.message = message
	def __str__(self):
		return str(self.message)

def sock_send(sock, s, length = None):
	wbytes = 0
	if length == None:
		length = len(s)
	while wbytes < length:
		try:
			wbytes += sock.send(s[wbytes: length])
		except socket.error as e:
			raise ZC_Error, 'errno %d: %s' %(e.errno, e.strerror)
	return wbytes

def sock_recv(sock, length):
	s = ''
	while len(s) < length:
		try:
			buf = sock.recv(length-len(s))
		except socket.error as e:
			raise ZC_Error, 'errno %d: %s' %(e.errno, e.strerror)
		if buf == "":
			break
		s += buf
	return s

def fd_send(fd, s, length = None):
	wbytes = 0
	if length == None:
		length = len(s)
	while wbytes < length:
		try:
			wbytes += os.write(fd, s[wbytes: length])
		except OSError as e:
			raise ZC_Error, 'errno %d: %s' %(e.errno, e.strerror)

def fd_recv(fd, length):
	s = ''
	while len(s) < length:
		try:
			buf = os.read(fd, length-len(s))
		except OSError as e:
			raise ZC_Error, 'errno %d: %s' %(e.errno, e.strerror)
		if buf == "":
			break
		s += buf
	return s

def split_lines(s):
	start = 0
	lines = []
	length = len(s)
	while start < length:
		try:
			idx = s.index('\n', start)
		except ValueError as e:
			break
		lines.append(s[start:idx+1])
		start = idx+1
	lines.append(s[start:length])
	return lines

