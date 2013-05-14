"""Config module
Copyright (C) Zhou Changrong"""
import sys
import os
import stat
import re
from zc_config_h import *
from zc_core_h import *
import zc_core

commands = [
	zc_command(
		name = 'include',
		type = ZC_ANY_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE1,
		set = zc_conf_include,
		describe = 'Include config file',
		key = None
		),
	zc_command(
		name = 'define',
		type = ZC_ANY_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE2,
		set = zc_conf_set_key_slot,
		describe = 'Define a marco',
		key = 'defines'
	),
	zc_command(
		name = 'set',
		type = ZC_ANY_CONF|ZC_CONF_DIRECT|ZC_CONF_TAKE2,
		set = zc_conf_set_key_slot,
		describe = 'Define a variable',
		key = 'sets'
	),
]

module = zc_module(
	name = 'config',
	version = '0.1.0',
	type = ZC_CONF_MODULE,
	commands = commands,
	ctx = None
	)
