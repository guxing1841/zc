"""\
Config common
Copyright (C) Zhou Changrong
"""
import os, stat, re, logging
ZC_VERSION           = "0.3.0"

ZC_CONF_NOARGS    = 0x00000001
ZC_CONF_TAKE1     = 0x00000002
ZC_CONF_TAKE2     = 0x00000004
ZC_CONF_TAKE3     = 0x00000008
ZC_CONF_TAKE4     = 0x00000010
ZC_CONF_TAKE5     = 0x00000020
ZC_CONF_TAKE6     = 0x00000040
ZC_CONF_TAKE7     = 0x00000080
ZC_CONF_MAX_ARGS  = 8
ZC_CONF_TAKE12    = (ZC_CONF_TAKE1|ZC_CONF_TAKE2)
ZC_CONF_TAKE13    = (ZC_CONF_TAKE1|ZC_CONF_TAKE3)
ZC_CONF_TAKE23    = (ZC_CONF_TAKE2|ZC_CONF_TAKE3)
ZC_CONF_TAKE123   = (ZC_CONF_TAKE1|ZC_CONF_TAKE2|ZC_CONF_TAKE3)
ZC_CONF_TAKE1234  = (ZC_CONF_TAKE1|ZC_CONF_TAKE2|ZC_CONF_TAKE3|ZC_CONF_TAKE4)
ZC_CONF_ARGS_NUMBER = 0x000000ff
ZC_CONF_1MORE       = 0x00000800
ZC_CONF_2MORE       = 0x00001000
ZC_CONF_MULTI       = 0x00002000
ZC_CONF_BLOCK       = 0x00004000
ZC_CONF_DIRECT      = 0x00008000
ZC_MAIN_CONF        = 0x01000000
ZC_ANY_CONF         = 0xFF000000

ZC_CONF_FLAG        = 0x00000002
ZC_CONF_INT         = 0x00000004
ZC_CONF_FLOAD       = 0x00000008
ZC_CONF_SIZE        = 0x00000010
ZC_CONF_ARRAY       = 0x00000016

ZC_ERROR = -1
ZC_OK = 0
ZC_CONF_FILE_DONE = 1
ZC_CONF_BLOCK_START = 2
ZC_CONF_BLOCK_DONE = 3
ZC_CONF_COMMAND_DONE = 4
ZC_CONF_WORD_DONE = 5
ZC_BUSY        = 100
ZC_SPARE       = 101
ZC_PREPARE     = 102
ZC_IGNORE      = 103

ZC_LOG_LEVELS = {'DEBUG' : logging.DEBUG, 'INFO' : logging.INFO, 'WARNING' : logging.WARNING, 'ERROR' : logging.ERROR, 'CRITICAL' : logging.CRITICAL}



ZC_CONF_MODULE = 0
ZC_CONF_ARGS_NUMBERS = [ZC_CONF_NOARGS, ZC_CONF_TAKE1, ZC_CONF_TAKE2, ZC_CONF_TAKE3, ZC_CONF_TAKE4, ZC_CONF_TAKE5, ZC_CONF_TAKE6, ZC_CONF_TAKE7]

def zc_parse_sec(arg):
	#m = re.match(r'^(-?(?:[\d]*\.[\d]+|[\d]+\.[\d]*|[\d]+))(.*)$', arg)
	m = re.match(r'^((?:[\d]*\.[\d]+|[\d]+\.[\d]*|[\d]+))(.*)$', arg)
	if m == None:
		raise ValueError, "invaild number"
	number = m.group(1)
	unit = m.group(2)
	scale = 1
	if unit  == 'm':
		scale = 60
	elif unit == 'h':
		scale = 60 * 60
	elif unit == 'd':
		scale = 60 * 60 * 24
	elif unit == 'w':
		scale = 60 * 60 * 24 * 7
	elif unit == 'M':
		scale = 60 * 60 * 24 * 30
	elif unit == 'y':
		scale = 60 * 60 * 24 * 365
	elif unit in ('s', ''):
		scale = 1
	elif unit == 'ms':
		scale = 0.001
	elif unit == 'us':
		scale = 0.000001
	elif unit == 'ns':
		scale = 0.000000001
	else:
		raise ValueError, "invaild unit"
	var = float(number) * scale
	return var

def zc_conf_check_slot(cf, cmd, conf):
	if cmd.key in cf.conf['ctx']:
		cf.log.error("Command '%s' is duplicated in %s at line %d: %d" %(cf.args[0], cf.cur['conf_file'], cf.cur['start_line'], cf.cur['start_col']))
		return ZC_ERROR
	return ZC_OK

	
		
def zc_conf_set_int_slot(cf, cmd, conf):
	if zc_conf_check_slot(cf, cmd, conf) != ZC_OK:
		return ZC_ERROR
	try:
		var = int(cf.args[1])
	except:
		cf.log.error("Can't parse '%s' in %s at line %d: %d" %(cf.args[1], cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
		return ZC_ERROR
	conf[cmd.key] = var
	return ZC_OK

def zc_conf_set_str_slot(cf, cmd, conf):
	if zc_conf_check_slot(cf, cmd, conf) != ZC_OK:
		return ZC_ERROR
	conf[cmd.key] = cf.args[1]
	return ZC_OK

def zc_conf_set_loglevel_slot(cf, cmd, conf):
	if zc_conf_check_slot(cf, cmd, conf) != ZC_OK:
		return ZC_ERROR
	level = ZC_LOG_LEVELS.get(cf.args[1].upper())
	if level == None:
		cf.log.error("Unsupport log level '%s' in %s at line %d: %d" %(cf.args[1], cf.cur['conf_file'], cf.cur['start_line'], cf.cur['start_col']))
		return ZC_ERROR
	conf[cmd.key] = level
	return ZC_OK

def zc_conf_set_flag_slot(cf, cmd, conf):
	if zc_conf_check_slot(cf, cmd, conf) != ZC_OK:
		return ZC_ERROR
	if cf.args[1].lower() in ('on', 'yes'):
		conf[cmd.key] = True
	elif cf.args[1].lower() in ('off', 'no'):
		conf[cmd.key] = False
	else:
		cf.log.error("Flag directive '%s' value '%s' error (Must be on, yes, off, no) in %s at line %d: %d" %(cf.args[0],  cf.args[1], cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
		return ZC_ERROR
	return ZC_OK

def zc_conf_set_key_slot(cf, cmd, conf):
	if cmd.key == None:
		cf.log.error("command '%s' is not defined in %s at line %d: %d" %(cf.args[1], cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
		return ZC_ERROR
	conf[cmd.key][cf.args[1]]=cf.args[2]
	return ZC_OK

def zc_conf_set_sec_slot(cf, cmd, conf):
	if zc_conf_check_slot(cf, cmd, conf) != ZC_OK:
		return ZC_ERROR
	try:
		var = zc_parse_sec(cf.args[1])
	except ValueError, e:
		cf.log.error("Can't parse '%s' %s in %s at line %d: %d" %(cf.args[1], e, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
		return ZC_ERROR
	conf[cmd.key] = var
	return ZC_OK

excape_table = {
	'0' : '\0',
	'\\' : '\\',
	'a' : '\a',
	'b' : '\b',
	'f' : '\f',
	'n' : '\n',
	'r' : '\r',
	't' : '\t',
	'v' : '\v',
	'\'' : '\'',
	'"' : '"',
	'?' : '\?',
	}
excape_sigle_table = {
	'\\' : '\\',
	'\'' : '\'',
	}


class zc_conf_file():
	def __init__(cf, buffer_size = 4096):
		cf.macro = False
		cf.cur = None
		cf.conf = None
		cf.main_conf = None
		cf.args = None
		cf.cmd = None
		cf.type = ZC_MAIN_CONF
		cf.log = None
		cf.buffer_size = buffer_size
		cf.commands = {}
		cf.modules = []
		cf.child_modules = {}
	def conf_handler(cf, rc):
		key = cf.args[0].lower()
		if key not in cf.commands:
			cf.log.error("Unkown command '%s' in %s at %d: %d" %(cf.args[0], cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
			return ZC_ERROR
		cf.cmd = cf.commands[key]
		ctx_index = cf.cmd.ctx_index
		if not (cf.cmd.type & cf.type):
			cf.log.error("Can't allowed here '%s' in %s at %d: %d" %(cf.cmd.name, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
			return ZC_ERROR
		if rc == ZC_CONF_BLOCK_START and not (cf.cmd.type & ZC_CONF_BLOCK):
			cf.log.error("'%s' is not block in %s at %d: %d" %(cf.cmd.name, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
			return ZC_ERROR
		if rc != ZC_CONF_BLOCK_START and (cf.cmd.type & ZC_CONF_BLOCK):
			cf.log.error("'%s' is block in %s at %d: %d" %(cf.cmd.name, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
			return ZC_ERROR
		if cf.cmd.type & ZC_CONF_1MORE:
			if len(cf.args) < 2:
				cf.log.error("Too less arguments '%s' in %s at %d: %d" %(cf.cmd.name, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
				return ZC_ERROR
		elif cf.cmd.type & ZC_CONF_2MORE:
			if len(cf.args) < 3:
				cf.log.error("Too less arguments '%s' in %s at %d: %d" %(cf.cmd.name, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
				return ZC_ERROR
		
		elif len(cf.args) > ZC_CONF_MAX_ARGS+1:
			cf.log.error("Too many arguments '%s' in %s at %d: %d" %(cf.cmd.name, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
			return ZC_ERROR
		elif not (cf.cmd.type & ZC_CONF_ARGS_NUMBERS[len(cf.args)-1]):
			cf.log.error("Invalid number arguments '%s' in %s at %d: %d" %(cf.cmd.name, cf.cur['conf_file'], cf.cur['start_line'],  cf.cur['start_col']))
			return ZC_ERROR
		if cf.cmd.set != None:
			if cf.cmd.type & ZC_CONF_DIRECT:
				return cf.cmd.set(cf, cf.cmd, cf.conf['ctx'])
			else:
				return cf.cmd.set(cf, cf.cmd, cf.conf['modules'][ctx_index])
		else:
			if cf.cmd.type & ZC_CONF_DIRECT:
				ctx = cf.conf['ctx']
			else:
				ctx = cf.conf['modules'][ctx_index]
			if cf.cmd.type & ZC_CONF_MULTI:
				if cf.cmd.key not in ctx:
					ctx[cf.cmd.key] = cf.args[1:]
				else:
					ctx[cf.cmd.key] += cf.args[1:]
			else:
				if cf.cmd.key in ctx:
					cf.log.error("Command '%s' is duplicated in %s at line %d: %d" %(cf.args[0], cf.cur['conf_file'], cf.cur['start_line'], cf.cur['start_col']))
					return ZC_ERROR
				else:
					ctx[cf.cmd.key] = cf.args[1:]
		return ZC_OK
			
	def file_parser(cf, conf_file = None):
		parse_file = 0
		parse_block = 1
		type = parse_file
		cur = cf.cur
		if conf_file == None:
			type = parse_block
		else:
			try:
				fh = open(conf_file, "r")
			except IOError, e:
				cf.log.error("Can't open config file: %s" %(e))
				return ZC_ERROR
			cur = {
				'fh' : None,
				'fd' : 0,
				'size' : 0,
				'offset' : 0,
				'info' : None,
				'buffer' : '',
				'buffer_pos' : 0,
				'buffer_last' : 0,
				'buffer_end' : cf.buffer_size,
				'line' : 1,
				'col' : 0,
				'start_line' : 1,
				'start_col' : 0,
				'conf_file' : conf_file 
				}
			cf.cur = cur
			cur['fh'] = fh
			cur['fd'] = fh.fileno()
			cur['info'] = os.fstat(fh.fileno())
			cur['size'] = cur['info'][stat.ST_SIZE]
			cur['offset'] = 0
		cf.cmd = None
		cf.args = []
		while True:
			rc = cf.read_token()
			if rc == ZC_ERROR:
				return ZC_ERROR
			if rc == ZC_CONF_BLOCK_DONE:
				if type != parse_block:
					cur['fh'].close()
					cf.log.error("Unexpecting \"}\" in %s at %d: %d" %(cf.cur['conf_file'], cf.cur['line'], cf.cur['col']))
					return ZC_ERROR
				if len(cf.args) == 0:
					return ZC_OK
			elif rc == ZC_CONF_FILE_DONE:
				cur['fh'].close()
				if type == parse_block:
					cf.log.error("Unexpected end of file, expecting \"}\" in %s at %d: %d" %(cf.cur['conf_file'], cf.cur['line'], cf.cur['col']))
					return ZC_ERROR
				elif type == parse_file:
					return ZC_OK
			if len(cf.args) > 0:
				if cf.conf_handler(rc) == ZC_ERROR:
					cur['fh'].close()
					return ZC_ERROR
				cf.cmd = None
				cf.args = []
				if rc == ZC_CONF_BLOCK_DONE:
					return ZC_OK
			elif rc == ZC_CONF_BLOCK_START:
				if cf.file_parser() == ZC_ERROR:
					return ZC_ERROR
		return ZC_OK
			
	def read_token(cf):
		global excape_table,excape_sigle_table
		cur = cf.cur
		comment = None
		quote = None
		word = None
		prev_char = None
		ch = None
		rc = ZC_OK
		macro = None
		macro_quoted = False
		ctx = cf.conf['ctx']
		while True:
			if cur['buffer_pos'] >= cur['buffer_last']:
				if cur['buffer_pos'] > 0:
					cur['buffer'] = cur['buffer'][cur['buffer_pos']:cur['buffer_last']]
					cur['buffer_last'] -= cur['buffer_pos']
					cur['buffer_pos'] = 0
				size = cur['size'] - cur['offset']
				if size > cur['buffer_end']-cur['buffer_last']:
					size = cur['buffer_end']-cur['buffer_last']
				if size == 0:
					if quote is not None:
						cf.log.error("Unexpected end of file, expecting \"%s\" in %s at %d" %(quote, cf.cur['conf_file'], cf.cur['line']))
						return ZC_ERROR
					if comment == '/*':
						cf.log.error("Unexpected end of file, expecting \"*/\" in %s at %d" %(cf.cur['conf_file'], cf.cur['line']))
						return ZC_ERROR
					if len(cf.args) > 0:
						cf.log.error("Unexpected end of file, expecting \";\" in %s at %d" %(cf.cur['conf_file'], cf.cur['line']))
						return ZC_ERROR
					return ZC_CONF_FILE_DONE
				buf = cur['fh'].read(size)
				if buf == '':
					cf.log.error("May be file is change")
					return ZC_ERROR
				cur['buffer'] += buf
				cur['buffer_last'] += len(buf)
				cur['offset'] += len(buf)
			prev_char = ch
			ch = cur['buffer'][cur['buffer_pos']:cur['buffer_pos']+1]
			cur['buffer_pos'] += 1
			cur['col'] += 1
			if ch == '\n':
				cur['line'] += 1
				cur['col'] = 0
				# end of '#' or '//'
				if comment in ('#', '//'):
					comment = None

			if comment in ('#', '//', '/*'):
				# end of '/*...*/'
				if comment == '/*' and prev_char == '*' and ch == '/':
					comment = None
					ch = None
				continue
			if macro is not None:
				if not macro_quoted:
					if ch == '{':
						macro_quoted = True
						continue
					elif macro == '' and ch == '%':
						if word is None:
							word = ch
						else:
							word += ch
						macro = None
						ch = None
						continue
				if macro_quoted and ch == '}':
					macro_quoted = False
				elif re.match(r'[a-zA-Z0-9_]', ch):
					macro += ch
					continue
				if macro_quoted:
					cf.log.error("Unexpected char '%s' in quoted macro in %s at %d:%d" %(ch, cur['conf_file'], cur['line'], cur['col']))
					return ZC_ERROR
				if macro == '':
					macro == None
				elif macro in ctx['defines']:
					if word is None:
						word = ctx['defines'][macro]
					else:
						word += ctx['defines'][macro]
					macro = None
					if ch == '}':
						continue
				else:
					cf.log.error("Undefine macro '%s' in %s at %d:%d" %(macro, cur['conf_file'], cur['line'], cur['col']))
					return ZC_ERROR
			if ch.isspace():
				if quote is None:
					if word is not None:
						cf.args.append(word)
						word = None
					continue
			if word is None and len(cf.args) == 0:
				cur['start_line'] = cur['line']
				cur['start_col'] = cur['col']
			if ch in ('"', '\'') and quote is None:
				quote = ch
				if word is None:
					word = ''
				continue
			elif ch == quote:
				quote = None
				continue
			elif ch == '%':
				if macro is None:
					macro = ''
					continue
				else:
					macro = None
			elif ch == '\\':
				continue
			elif quote is None:
				if prev_char == '/':
					if ch == '/':
						comment = '//'
						continue
					if ch == '*':
						comment = '/*'
						continue
					if word is None:
						word = prev_char
					else:
						word += prev_char
				if comment is None and  prev_char == '*' and ch == '/':
					cf.log.error("Unexpected '*/' in %s at %d" %(cur['conf_file'], cur['line']))
					return ZC_ERROR
				if prev_char == '\\':
					if (quote == '\'' and ch in excape_sigle_table) or (quote == '"' and ch in excape_table):
						word += excape_table[ch]
						continue
					elif quote == '\'':
						word += prev_char
				elif ch == '#':
					comment = '#'
					continue
				elif ch == '/':
					continue
				elif ch == ';':
					rc = ZC_CONF_COMMAND_DONE
				elif ch == '{':
					rc = ZC_CONF_BLOCK_START
				elif ch  == '}':
					rc = ZC_CONF_BLOCK_DONE
			if rc is not ZC_OK:
				if prev_char in ('%', '/'):
					word += prev_char
				if word is not None:
					cf.args.append(word)
					word = None
				return rc
			if word is None:
				word = ch
			else:
				word += ch
		return ZC_OK

def zc_conf_include(cf, cmd, conf):
	prev = cf.cur
	if cf.file_parser(cf.args[1]) == ZC_ERROR:
		return ZC_ERROR
	cf.cur = prev
	return ZC_OK

def zc_dict_set_no_has(dt1, dt2, *keys):
	for k in keys:
		if k not in dt1 and k in dt2:
			dt1[k] = dt2[k]
def zc_dict_get_gt(dt, k, m, d=None):
	if k in dt:
		if dt[k] > m:
			return dt[k]
	return d
def zc_dict_get_ge(dt, k, m, d=None):
	if k in dt:
		if dt[k] >= m:
			return dt[k]
	return d
