# -*- encoding: utf8 -*-
__version__ = '0.1'
__author__ = 'Joshua Forman'

import pydoc
import logging
import pickle
import sys
import traceback
import zmq

class ZRPCServer(object):
	serializer = pickle
	sock_opt = []
	sock_bind = True
	runner = None

	def __init__(self, address, methods=None):
		self.logger = logging.getLogger('zrpc.ZRPCServer')
		ctx = zmq.Context.instance()
		self.sock = ctx.socket(zmq.REP)
		for option in self.sock_opt:
			self.sock.setsockopt(*option)
		if self.sock_bind:
			self.logger.debug('Binding to: %s', address)
			self.sock.bind(address)
		else:
			self.logger.debug('Connecting to: %s', address)
			self.sock.connect(address)
		self.methods = methods or {}
		self.methods['list_functions'] = self._list_methods
		self.methods['documentation'] = self._get_doc

	def _list_methods(self):
		"""
		Retrieve list of functions supported by the server
		"""
		return self.methods.keys()

	def _get_doc(self, func_name):
		"""
		Retrieve docstring of the function identified by `func_name`
		Params:

		func_name -> 
			the function name used when adding it to the server's methods
		"""
		try:
			func = self.methods[func_name]
		except KeyError:
			raise NotImplementedError(func_name)
		else:
			documentation = pydoc.render_doc(func, title='%s')
			return documentation

	def add_function(self, func_name, function):
		self.methods[func_name] = function

	def remove_function(self, func_name):
		self.methods.pop(func_name, None)

	def _run(self):
		while True:
			message = self.sock.recv_multipart()
			func_name, args, kwargs = message[0], self.serializer.loads(message[1]), self.serializer.loads(message[2])
			if func_name == 'kill_server':
				self.sock.send_pyobj({'status': True, 'result': True})
				self.sock.close()
				break
			try:
				func = self.methods[func_name]
			except KeyError:
				self.sock.send_pyobj({'status': None, 'result': None})
			try:
				results = func(*args, **kwargs)
			except Exception, error:
				formatted_traceback = traceback.format_exc()
				self.logger.error(
					'%s%s',
					traceback.format_exception_only(error.__class__, error)[0],
					''.join(formatted_traceback)
				)
				self.sock.send_pyobj({'status': False, 'result': (error, formatted_traceback)})
			else:
				self.sock.send_pyobj({'status': True, 'result': results})

	def start(self):
		if self.runner:
			self.runner(self._run)
		else:
			self._run()

class ZRPCClient(object):
	serializer = pickle
	sock_opt = []
	sock_bind = False

	def __init__(self, address):
		self.logger = logging.getLogger('zrpc.ZRPCClient')
		ctx = zmq.Context.instance()
		self.sock = ctx.socket(zmq.REQ)
		for option in self.sock_opt:
			self.sock.setsockopt(*option)
		if self.sock_bind:
			self.logger.debug('Binding to: %s', address)
			self.sock.bind(address)
		else:
			self.logger.debug('Connecting to: %s', address)
			self.sock.connect(address)

	def _call(self, method, *args, **kwargs):
		self.sock.send_multipart([method, self.serializer.dumps(args), self.serializer.dumps(kwargs)])
		response = self.sock.recv_pyobj()
		if response['status'] is None:
			self.logger.warn('Command %r does not exist', method)
			return None, None
		elif response['status'] is False:
			error_value, error_traceback = response['result']
			self.logger.error(
				'%s%s',
				traceback.format_exception_only(error_value.__class__, error_value)[0],
				''.join(error_traceback)
			)
			return False, error_value
		else:
			return True, response['result']

	def __getattr__(self, method):
		def rpc_call(*args, **kwargs):
			suppress_errors = bool(kwargs.pop('_suppress', None))
			status, result = self._call(method, *args, **kwargs)
			if not status:
				if suppress_errors:
					return None
				if result is None:
					raise NotImplementedError(method)
				else:
					raise result
			return result
		return rpc_call


def demonstration():
	import time, sys
	import threading
	#import gevent
	#import gevent_zeromq
	logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
	#gevent_zeromq.monkey_patch()
	server = ZRPCServer('inproc://rpc_server')
	def spawn_thread(func):
		threading.Thread(target=func).start()
	server.runner = spawn_thread
	#server.runner = gevent.spawn
	def echo(statement, foo='a', bar=None, **kwargs):
		"Basic echo command"
		return statement
	class FuncDir(object):
		def something(self, *args, **kwargs):
			def foobar():
				return 10 * self.raise_error()
			return foobar()

		def raise_error(self):
			"Function which raises a NameError"
			print foo

	funcdir = FuncDir()
	server.add_function('echo', echo)
	server.add_function('error', funcdir.something)
	server.start()
	time.sleep(1)
	client = ZRPCClient('inproc://rpc_server')

	print "\nListing functions registered with server"
	print '=' * 80
	functions = client.list_functions()
	print functions

	print '*' * 80
	print "\nGetting documentation of a registered function: %s" % functions[-1]
	print '=' * 80
	print client.documentation(functions[-1])

	print '*' * 80
	print "\nExample remote call"
	print '=' * 80
	print client.echo('Hello World')

	print '*' * 80
	print "\nCall causes error which is suppressed"
	print '=' * 80
	try:
		client.error(_suppress=True)
	except NameError:
		print "Caught remote exception"
	else:
		print "Error was ignored"

	print '*' * 80
	print "\nCall causes error which is reraised"
	print '=' * 80
	try:
		client.error()
	except NameError:
		print "Caught remote exception"
		print traceback.format_exc()
	else:
		print "Error was ignored"

	print '*' * 80
	print "\nKilling server"
	print '=' * 80
	client.kill_server()
	print '*' * 80

def runner():
	try:
		address = sys.argv[1]
		function_name = sys.argv[2]
	except IndexError:
		print "USAGE %s: ADDRESS FUNCTION ARG1 ARG2 ... KEY1=VALUE1 KEY2=VALUE2 ..." % sys.argv[0]
		sys.exit(1)
	args = []
	kwargs = {}
	for arg in sys.argv[3:]:
		arg_name, _, arg_value = arg.partition('=')
		if arg_value:
			kwargs[arg_name] = arg_value
		else:
			args.append(arg_name)

	client = ZRPCClient(address)
	print getattr(client, function_name)(*args, **kwargs)

def sample_server():
	try:
		address = sys.argv[2]
	except IndexError:
		print "USAGE %s server: ADDRESS"
		sys.exit(1)
	server = ZRPCServer(address)
	def echo(*args, **kwargs):
		"Basic echo command"
		return args, kwargs
	class FuncDir(object):
		def something(self, *args, **kwargs):
			def foobar():
				return 10 * self.raise_error()
			return foobar()

		def raise_error(self):
			"Function which raises a NameError"
			print foo

	funcdir = FuncDir()
	server.add_function('echo', echo)
	server.add_function('error', funcdir.something)
	server.start()

if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
	try:
		mode = sys.argv[1]
	except IndexError:
		demonstration()
	else:
		if mode == 'client':
			runner()
		else:
			sample_server()
