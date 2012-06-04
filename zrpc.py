# -*- encoding: utf8 -*-
__version__ = '0.1'
__author__ = 'Joshua Forman'

import pydoc
import logging
import pickle
import sys
import traceback
import zmq

class ZBaseSocket(object):
	sock_opt = []
	sock_bind = False
	socket_type = None

	def __init__(self, address, logger_name='zrpc.ZBaseSocket'):
		self.logger = logging.getLogger(logger_name)
		ctx = zmq.Context.instance()
		self.sock = ctx.socket(self.socket_type)
		for option in self.sock_opt:
			self.sock.setsockopt(*option)
		if self.sock_bind:
			self.logger.debug('Binding to: %s', address)
			self.sock.bind(address)
		else:
			self.logger.debug('Connecting to: %s', address)
			self.sock.connect(address)

class ZBaseServer(ZBaseSocket):
	serializer = pickle
	sock_opt = []
	sock_bind = True
	socket_type = zmq.REP
	runner = None

	def __init__(self, address, methods=None, logger_name='zrpc.ZBaseServer'):
		ZBaseSocket.__init__(self, address, logger_name=logger_name)
		self.methods = methods or {}
		self.methods['list_functions'] = self._list_methods
		self.methods['documentation'] = self._get_doc
		self.methods['echo'] = self._echo

	def _list_methods(self):
		"""
		Retrieve list of functions supported by the server
		"""
		return self.methods.keys()

	def _echo(self, statement):
		"""
		Echo input
		"""
		return statement

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

	def _execute_with_tracebacks(self, func, args, kwargs):
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

	def _run(self):
		raise NotImplementedError

	def add_function(self, func_name, function):
		self.methods[func_name] = function

	def remove_function(self, func_name):
		self.methods.pop(func_name, None)

	def get_message(self):
		message = self.sock.recv_multipart()
		func_name, args, kwargs = message[0], self.serializer.loads(message[1]), self.serializer.loads(message[2])
		return (func_name, args, kwargs)

	def start(self):
		if self.runner:
			self.runner(self._run)
		else:
			self._run()

class ZRPCServer(ZBaseServer):
	def __init__(self, address, methods=None):
		ZBaseServer.__init__(self, address, methods=methods, logger_name='zrpc.ZRPCServer')

	def _run(self):
		while True:
			func_name, args, kwargs = self.get_message()
			if func_name == 'kill_server':
				self.sock.send_pyobj({'status': True, 'result': True})
				self.sock.close()
				break
			try:
				func = self.methods[func_name]
			except KeyError:
				self.sock.send_pyobj({'status': None, 'result': None})
			else:
				self._execute_with_tracebacks(func, args, kwargs)

class ZRPCClient(ZBaseSocket):
	serializer = pickle
	sock_opt = []
	sock_bind = False
	socket_type = zmq.REQ

	def __init__(self, address):
		ZBaseSocket.__init__(self, address, logger_name='zrpc.ZRPCClient')

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

class ZWorker(ZBaseServer):
	"""
	ZWorker allows executing functions in a more 'fire-and-forget' way.
	A response is returned right away, then the remote function is executed
	"""
	socket_type = zmq.PULL
	sock_bind = False

	def __init__(self, address, methods=None):
		ZBaseServer.__init__(self, address, methods=methods, logger_name='zrpc.ZWorker')

	def _execute_async(self, func, args, kwargs):
		try:
			results = func(*args, **kwargs)
		except Exception, error:
			formatted_traceback = traceback.format_exc()
			self.logger.error(
				'%s%s',
				traceback.format_exception_only(error.__class__, error)[0],
				''.join(formatted_traceback)
			)
		else:
			self.logger.debug("%s(*%r, **%r) -> %r", func_name, args, kwargs, results)

	def _run(self):
		while True:
			func_name, args, kwargs = self.get_message()
			if func_name == 'kill_server':
				self.sock.close()
				break
			try:
				func = self.methods[func_name]
			except KeyError:
				pass
			else:
				self._execute_async(func, args, kwargs)

class ZMaster(ZBaseSocket):
	serializer = pickle
	sock_opt = []
	sock_bind = False
	socket_type = zmq.PUSH

	def __init__(self, address):
		ZBaseSocket.__init__(self, address, logger_name='zrpc.ZRPCClient')

	def _call(self, method, *args, **kwargs):
		self.sock.send_multipart([method, self.serializer.dumps(args), self.serializer.dumps(kwargs)])

	def __getattr__(self, method):
		self._call(method, *args, **kwargs)


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
	print "\nCall to undefined remote function"
	print '=' * 80
	try:
		client.spam()
	except NotImplementedError:
		print "Caught remote exception"
		print traceback.format_exc()
	else:
		print "Error was ignored"

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
