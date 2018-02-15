
from distutils.command.build import build as _build
import subprocess

import setuptools


# This class handles the pip install mechanism.
class build(_build):  # pylint: disable=invalid-name
  """A build command class that will be invoked during package install.
  The package built using the current setup.py will be staged and later
  installed in the worker using `pip install package'. This class will be
  instantiated during install for this specific scenario and will trigger
  running the custom commands specified.
  """
  sub_commands = _build.sub_commands + [('CustomCommands', None)]


# Some custom command to run during setup. The command is not essential for this
# workflow. It is used here as an example. Each command will spawn a child
# process. Typically, these commands will include steps to install non-Python
# packages. For instance, to install a C++-based library libjpeg62 the following
# two commands will have to be added:
#
#     ['apt-get', 'update'],
#     ['apt-get', '--assume-yes', install', 'libjpeg62'],
#
# First, note that there is no need to use the sudo command because the setup
# script runs with appropriate access.
# Second, if apt-get tool is used then the first command needs to be 'apt-get
# update' so the tool refreshes itself and initializes links to download
# repositories.  Without this initial step the other apt-get install commands
# will fail with package not found errors. Note also --assume-yes option which
# shortcuts the interactive confirmation.
#
# The output of custom commands (including failures) will be logged in the
# worker-startup log.
CUSTOM_COMMANDS = [
    ['echo', 'Custom command worked!']]


class CustomCommands(setuptools.Command):
  """A setuptools Command class able to run arbitrary commands."""

  def initialize_options(self):
    pass

  def finalize_options(self):
    pass

  def RunCustomCommand(self, command_list):
    print 'Running command: %s' % command_list
    p = subprocess.Popen(
        command_list,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # Can use communicate(input='y\n'.encode()) if the command run requires
    # some confirmation.
    stdout_data, _ = p.communicate()
    print 'Command output: %s' % stdout_data
    if p.returncode != 0:
      raise RuntimeError(
          'Command %s failed: exit code: %s' % (command_list, p.returncode))

  def run(self):
    for command in CUSTOM_COMMANDS:
      self.RunCustomCommand(command)


REQUIRED_PACKAGES = [
    'apache-beam==2.1.0',
    'avro==1.8.2',
    'beautifulsoup4==4.6.0',
    'cachetools==2.0.1',
    'certifi==2018.1.18',
    'chardet==3.0.4',
    'crcmod==1.7',
    'dill==0.2.6',
    'enum34==1.1.6',
    'funcsigs==1.0.2',
    'future==0.16.0',
    'futures==3.2.0',
    'gapic-google-cloud-pubsub-v1==0.15.4',
    'google-api-core==0.1.4',
    'google-apitools==0.5.11',
    'google-auth==1.4.0',
    'google-auth-httplib2==0.0.3',
    'google-cloud-bigquery==0.25.0',
    'google-cloud-core==0.25.0',
    'google-cloud-language==1.0.0',
    'google-cloud-pubsub==0.26.0',
    'google-gax==0.15.16',
    'googleapis-common-protos==1.5.3',
    'googledatastore==7.0.1',
    'grpc-google-iam-v1==0.11.4',
    'grpcio==1.9.1',
    'httplib2==0.9.2',
    'idna==2.6',
    'mock==2.0.0',
    'oauth2client==3.0.0',
    'pbr==3.1.1',
    'ply==3.8',
    'proto-google-cloud-datastore-v1==0.90.4',
    'proto-google-cloud-pubsub-v1==0.15.4',
    'protobuf==3.3.0',
    'pyasn1==0.4.2',
    'pyasn1-modules==0.2.1',
    'pytz==2018.3',
    'PyYAML==3.12',
    'requests==2.18.4',
    'rsa==3.4.2',
    'six==1.10.0',
    'typing==3.6.4',
    'urllib3==1.22'
    ]


setuptools.setup(
    name='maria',
    version='0.0.1',
    description='DataFlow Python Pipeline',
    install_requires=REQUIRED_PACKAGES,
    py_modules=['pipeline_options'],
    cmdclass={
        'build': build,
        'CustomCommands': CustomCommands,
        }
    )
