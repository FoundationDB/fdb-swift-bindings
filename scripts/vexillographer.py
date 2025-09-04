#
# vexillographer.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2016-2025 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

'''
This file provides logic for generating options code based on the fdb.options
file provided by FoundationDB.

To use Vexillographer, you must define an emitter. An emitter is a class that
provides language-specific implementations of the options syntax. The emitter
must define the following methods:

::

    class Emitter(object):
        def print_header_warning(self):
            # This method prints the warning at the top of the file that tells people
            # not to edit it by hand.

            pass

        def print_footer(self):
            # This method prints the warning at the end of the file.

            pass

        def write_scope_start(self, name, signed):
            # This method writes the beginning of the type containing a kind of
            # option.

            # :param name: The name of the scope
            # :param signed: Whether these options can have negative values.

            pass

        def write_scope_end(self):
            # This method writes the end of the type containing a kind of option.

            pass

        def write_option(self, name, code, description, scope_name, deprecated):
            # This method writes a single option.
            #
            # :param scope_name: The name of the type that this option is part of.
            # :param name: The name of the option.
            # :param code: The numeric value for the option.
            # :param description: A comment describing the option.
            # :param deprecated: Whether the option has been deprecated.
            #

            pass

There are two ways you can run Vexillographer:

1.  Call :code:`Vexillographer.run`, passing a function that takes in a file
    object and returns an emitter for writing to that file. This will parse
    the standard arguments for Vexillographer and run the command to generate
    the file.
2.  Initialize a Vexillographer instance with an open file containing the
    options XML from fdb.options, and an emitter that will write to your output
    file. Then call :code:`write_file` on that Vexillographer instance.
'''

from argparse import ArgumentParser
import platform
from xml.etree import ElementTree

class Vexillographer(object):
    '''
    This class runs the Vexillographer script.
    '''

    def __init__(self, option_file, emitter):
        '''
        This initializer creates a Vexillographer run.

        :param option_file: The file that we are reading options from.
        :param emitter: The emitter object that will write the code.
        '''

        self.option_file = option_file
        self.emitter = emitter

    @staticmethod
    def run(emitter):
        '''
        This method reads the arguments for the script and executes it.
        '''

        if platform.system() == 'Darwin':
            default_options_path = '/usr/local/include/foundationdb/fdb.options'
        else:
            default_options_path = '/usr/include/fdb.options'
        parser = ArgumentParser(
            description='Generate options code for FoundationDB Swift bindings'
        )
        parser.add_argument(
            '--option-file',
            help='Path to the fdb.options file to use as the input.',
            default=default_options_path
        )
        parser.add_argument(
            '--generated-file',
            help='Path to the Swift file to write the options to',
            default='Sources/FoundationDB/Options.swift'
        )

        arguments = parser.parse_args()

        with open(arguments.option_file) as option_file:
            with open(arguments.generated_file, 'w') as generated_file:
                Vexillographer(option_file, emitter(generated_file)).write_file()

    def write_file(self):
        '''
        This method writes the options file.
        '''

        self.emitter.print_header_warning()

        input_xml = ElementTree.parse(self.option_file)
        for scope_node in input_xml.getroot():
            scope_name = scope_node.attrib['name']
            signed = scope_name == 'StreamingMode'
            self.emitter.write_scope_start(
                scope_name,
                signed=signed
            )

            for option_node in scope_node:
                description = option_node.attrib.get('description', '')
                deprecated = description == 'Deprecated'
                self.emitter.write_option(
                    option_node.attrib['name'],
                    int(option_node.attrib['code']),
                    description,
                    scope_name=scope_name,
                    deprecated=deprecated
                )

            self.emitter.write_scope_end()

        self.emitter.print_footer()
