"""\
HTTP common
Copyright (C) Zhou Changrong
"""
import pycurl, StringIO, time
from zc_common import *
class httprequest:
	def __init__(self, **args):
		self.c = None
		self.proxy_host = None
		self.proxy_port = None
		self.user_agent = None
		self.post_data = None
		self.headers = None
		self.method = None
		self.max_redirs = None
		self.followlocation = None
		self.connect_timeout = 0
		self.timeout = 0
		self.verbose = None
		self.version = None
		self.userpwd = None
		for key in args:
			if key == 'proxy_host':
				self.proxy_host = args['proxy_host']
			elif key == 'proxy_port':
				self.proxy_port = args['proxy_port']
			elif key == 'user_agent':
				self.user_agent = args['user_agent']
			elif key == 'post_data':
				self.post_data = args['post_data']
			elif key == 'headers':
				self.headers = args['headers']
			elif key == 'method':
				self.method = args['method']
			elif key == 'max_redirs':
				self.max_redirs = args['max_redirs']
			elif key == 'followlocation':
				self.fllowlocation = args['followlocation']
			elif key == 'connect_timeout':
				self.connect_timeout = args['connect_timeout']
			elif key == 'timeout':
				self.timeout = args['timeout']
			elif key == 'verbose':
				self.verbose = args['verbose']
			elif key == 'userpwd':
				self.userpwd = args['userpwd']
			else:
				raise TypeError, 'http_request() got an unexpected keyword argument \'%s\'' %(key)
		self.c = pycurl.Curl()
	def request(self, url = None, **args):
		c = self.c
		proxy_host = self.proxy_host
		proxy_port = self.proxy_port
		user_agent = self.user_agent
		post_data = self.post_data
		headers = self.headers
		method = self.method
		max_redirs = self.max_redirs
		followlocation = self.followlocation
		connect_timeout = self.connect_timeout
		timeout = self.timeout
		verbose = self.verbose
		version = self.version
		userpwd = self.userpwd
		result = {}
		for key in args:
			if key == 'proxy_host':
				proxy_host = args['proxy_host']
			elif key == 'proxy_port':
				proxy_port = args['proxy_port']
			elif key == 'user_agent':
				user_agent = args['user_agent']
			elif key == 'post_data':
				post_data = args['post_data']
			elif key == 'headers':
				headers = args['headers']
			elif key == 'method':
				method = args['method']
			elif key == 'max_redirs':
				max_redirs = args['max_redirs']
			elif key == 'followlocation':
				fllowlocation = args['followlocation']
			elif key == 'connect_timeout':
				connect_timeout = args['connect_timeout']
			elif key == 'timeout':
				timeout = args['timeout']
			elif key == 'verbose':
				verbose = args['verbose']
			elif key == 'version':
				version = args['version']
			elif key == 'userpwd':
				userpwd = args['userpwd']
			else:
				raise TypeError, 'http_request() got an unexpected keyword argument \'%s\'' %(key)
		c.setopt(c.URL, url)
		if proxy_host != None:
			c.setopt(c.PROXY, proxy_host)
			if proxy_port != None and proxy_port != 0:
				c.setopt(c.PROXYPORT, proxy_port)
		if user_agent != None:
			c.setopt(c.USERAGENT, user_agent)
		if headers != None and len(headers) > 0:
			c.setopt(c.HTTPHEADER, headers)
		if method != None:
			c.setopt(c.CUSTOMREQUEST, method)
		if method == 'POST' and post_data != None:
			c.setopt(c.POSTFIELDS, post_data)
		if followlocation != None:
			c.setopt(c.FOLLOWLOCATION, followlocation)
		if verbose != None:
			c.setopt(c.VERBOSE, verbose)
		if version != None:
			c.setopt(c.HTTP_VERSION, version)
		if userpwd != None:
			c.setopt(c.USERPWD, userpwd)
		if method == 'HEAD':
			c.setopt(c.NOBODY, True)
		b = StringIO.StringIO()
		h = StringIO.StringIO()
		c.setopt(c.WRITEFUNCTION, b.write)
		c.setopt(c.HEADERFUNCTION, h.write)
		c.setopt(c.HEADER, False)
		if timeout > 0:
			c.setopt(c.TIMEOUT, timeout)
		if connect_timeout > 0:
			c.setopt(c.CONNECTTIMEOUT, connect_timeout)
		c.perform()
		result['code'] = c.getinfo(c.HTTP_CODE)
		result['body'] = b.getvalue()
		result['header'] = h.getvalue()
		b.close()
		h.close()
		result['header_size'] = c.getinfo(c.HEADER_SIZE)
		result['body_size'] = len(result['body'])
		result['total_time'] = c.getinfo(c.TOTAL_TIME)
		result['speed_download'] = c.getinfo(c.SPEED_DOWNLOAD)
		result['speed_upload'] = c.getinfo(c.SPEED_UPLOAD)
		result['size_download'] = c.getinfo(c.SIZE_DOWNLOAD)
		result['size_upload'] = c.getinfo(c.SIZE_UPLOAD)
		return result
	def close(self):
		c = self.c
		if c != None:
			c.close()
class mhttprequest:
	handles = {}
	def __init__(self, line_out = None):
		self.line_out = line_out
		self.m = pycurl.CurlMulti()
	def add_request(self, url = None, **args):
		c = pycurl.Curl()
		proxy_host = None
		proxy_port = None
		user_agent = None
		post_data  = None
		headers    = None
		method     = None
		max_redirs = None
		followlocation = None
		connect_timeout = None
		timeout = 0
		verbose = 0
		version = None
		userpwd = None
		other_data = None
		result = {}
		for key in args:
			if key == 'proxy_host':
				proxy_host = args['proxy_host']
			elif key == 'proxy_port':
				proxy_port = args['proxy_port']
			elif key == 'user_agent':
				user_agent = args['user_agent']
			elif key == 'post_data':
				post_data = args['post_data']
			elif key == 'headers':
				headers = args['headers']
			elif key == 'method':
				method = args['method']
			elif key == 'max_redirs':
				max_redirs = args['max_redirs']
			elif key == 'followlocation':
				fllowlocation = args['followlocation']
			elif key == 'connect_timeout':
				connect_timeout = args['connect_timeout']
			elif key == 'timeout':
				timeout = args['timeout']
			elif key == 'verbose':
				verbose = args['verbose']
			elif key == 'version':
				version = args['version']
			elif key == 'userpwd':
				userpwd = args['userpwd']
			elif key == 'other_data':
				other_data = args['other_data']
			else:
				raise TypeError, 'add_request() got an unexpected keyword argument \'%s\'' %(key)
		c.setopt(c.URL, url)
		if proxy_host != None:
			c.setopt(c.PROXY, proxy_host)
			if proxy_port != None and proxy_port != 0:
				c.setopt(c.PROXYPORT, proxy_port)
		if user_agent != None:
			c.setopt(c.USERAGENT, user_agent)
		if headers != None and len(headers) > 0:
			c.setopt(c.HTTPHEADER, headers)
		if method != None:
			c.setopt(c.CUSTOMREQUEST, method)
		if method == 'POST' and post_data != None:
			c.setopt(c.POSTFIELDS, post_data)
		if followlocation != None:
			c.setopt(c.FOLLOWLOCATION, followlocation)
		if verbose != None:
			c.setopt(c.VERBOSE, verbose)
		if version != None:
			c.setopt(c.HTTP_VERSION, version)
		if userpwd != None:
			c.setopt(c.USERPWD, userpwd)
		if method == 'HEAD':
			c.setopt(c.NOBODY, True)
		b = StringIO.StringIO()
		h = StringIO.StringIO()
		c.setopt(c.WRITEFUNCTION, b.write)
		c.setopt(c.HEADERFUNCTION, h.write)
		c.setopt(c.HEADER, False)
		if timeout > 0:
			c.setopt(c.TIMEOUT, timeout)
		if connect_timeout > 0:
			c.setopt(c.CONNECTTIMEOUT, connect_timeout)
		self.handles[c]=[b, h, other_data]
		return None
	def perform(self, max_connections = 0):
		handle_list = self.handles.keys()
		n = len(handle_list)
		current_connections = 0
		i = 0
		while True:
			while i<n and (current_connections<max_connections or max_connections == 0):
				self.m.add_handle(handle_list[i])
				self.handles[handle_list[i]].append(time.time())
				i+=1
				current_connections += 1
			ret, num = self.m.perform()
			if ret == pycurl.E_CALL_MULTI_PERFORM:
				continue
			ret, handlers, others = self.m.info_read()
			now = time.time()
			for c in handlers:
				self.m.remove_handle(c)
				b, h, other_data, start = self.handles[c]
				total_time = now - start
				result = {}
				result['code'] = c.getinfo(c.HTTP_CODE)
				result['body'] = b.getvalue()
				result['header'] = h.getvalue()
				b.close()
				h.close()
				result['header_size'] = c.getinfo(c.HEADER_SIZE)
				result['body_size'] = len(result['body'])
				#result['total_time'] = c.getinfo(c.TOTAL_TIME)
				result['total_time'] = total_time
				result['speed_download'] = c.getinfo(c.SPEED_DOWNLOAD)
				result['speed_upload'] = c.getinfo(c.SPEED_UPLOAD)
				result['size_download'] = c.getinfo(c.SIZE_DOWNLOAD)
				result['size_upload'] = c.getinfo(c.SIZE_UPLOAD)
				self.line_out(result, other_data)
				c.close()
				del(self.handles[c])
				current_connections -= 1
			for o in others:
				c, code, msg = o
				self.m.remove_handle(c)
				b, h, other_data, start = self.handles[c]
				total_time = now - start

				result = {}
				result['code'] = c.getinfo(c.HTTP_CODE)
				result['body'] = b.getvalue()
				result['header'] = h.getvalue()
				b.close()
				h.close()
				result['header_size'] = c.getinfo(c.HEADER_SIZE)
				result['body_size'] = len(result['body'])
				result['total_time'] = total_time
				result['speed_download'] = c.getinfo(c.SPEED_DOWNLOAD)
				result['speed_upload'] = c.getinfo(c.SPEED_UPLOAD)
				result['size_download'] = c.getinfo(c.SIZE_DOWNLOAD)
				result['size_upload'] = c.getinfo(c.SIZE_UPLOAD)
				self.line_out(result, other_data, code, msg)
				c.close()
				del(self.handles[c])
				current_connections -= 1
			#print num, i, n, current_connections, max_connections
			if num == 0 and i == n:
				break
			ret = self.m.select(1.0)
	def close(self):
		for c in self.handles.keys():
			if c != None:
				c.close()
		m = self.m
		if m != None:
			m.close()
