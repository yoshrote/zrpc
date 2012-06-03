from setuptools import setup
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

setup(name='zrpc',
      version='0.1',
      description="0mq RPC client and server",
      long_description=README,
      classifiers=[
            'Intended Audience :: Developers',
            'Development Status :: 4 - Beta',
            'Programming Language :: Python',
            'License :: OSI Approved :: BSD License',
      ],
      keywords='zmq rpc',
      author='Joshua Forman',
      author_email='josh@yoshrote.com',
      url='',
      license='BSD',
      py_modules=['zrpc'],
      include_package_data=True,
      zip_safe=False,
      install_requires=['zmq'],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
