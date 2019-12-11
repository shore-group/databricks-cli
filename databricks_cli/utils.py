# Databricks CLI
# Copyright 2017 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import time
import traceback
from StringIO import StringIO
from multiprocessing import Event, Process
from json import dumps as json_dumps, loads as json_loads

import click
import six
from requests.exceptions import HTTPError

from databricks_cli.click_types import ContextObject

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
DEBUG_MODE = False


def eat_exceptions(function):
    @six.wraps(function)
    def decorator(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except HTTPError as exception:
            if exception.response.status_code == 401:
                error_and_quit('Your authentication information may be incorrect. Please '
                               'reconfigure with ``dbfs configure``')
            else:
                error_and_quit(exception.response.content)
        except Exception as exception: # noqa
            if not DEBUG_MODE:
                error_and_quit('{}: {}'.format(type(exception).__name__, str(exception)))
    decorator.__doc__ = function.__doc__
    return decorator


def error_and_quit(message):
    ctx = click.get_current_context()
    context_object = ctx.ensure_object(ContextObject)
    if context_object.debug_mode:
        traceback.print_exc()
    click.echo('Error: {}'.format(message))
    sys.exit(1)


def pretty_format(json):
    return json_dumps(json, indent=2)


def json_cli_base(json_file, json, api, print_response=True):
    """
    Takes json_file or json string and calls an function "api" with the json
    deserialized
    """
    if not (json_file is None) ^ (json is None):
        raise RuntimeError('Either --json-file or --json should be provided')
    if json_file:
        with open(json_file, 'r') as f:
            json = f.read()
    res = api(json_loads(json))
    if print_response:
        click.echo(pretty_format(res))


def truncate_string(s, length=100):
    if len(s) <= length:
        return s
    return s[:length] + '...'


class InvalidConfigurationError(RuntimeError):
    @staticmethod
    def for_profile(profile):
        if profile is None:
            return InvalidConfigurationError(
                'You haven\'t configured the CLI yet! '
                'Please configure by entering `{} configure`'.format(sys.argv[0]))
        return InvalidConfigurationError(
            ('You haven\'t configured the CLI yet for the profile {profile}! '
             'Please configure by entering '
             '`{argv} configure --profile {profile}`').format(
                profile=profile, argv=sys.argv[0]))


class LoadingBar(object):

    @staticmethod
    def _displayer(msg, interval, width, fill_char, stop_event):
        bar = ' ' * width          # pylint: disable=C0102
        bars = []
        for i in range(width):
            bars.append(bar[:i] + fill_char + bar[i + 1:])
        bars += list(reversed(bars[1:-1]))
        n = len(bars)
        i = 0
        while True:
            if stop_event.is_set():
                sys.stdout.write('\n')
                return
            sys.stdout.write('\r{} [{}]'.format(msg, bars[i]))
            sys.stdout.flush()
            i = (i + 1) % n
            time.sleep(interval)

    def __init__(self, msg='Loading', interval=.5, width=7, fill_char='.'):
        self.original_stdout = sys.stdout
        self.dummy_stdout = StringIO()
        self.stop_event = Event()
        self.displayer = Process(
            target=self._displayer,
            args=(msg, interval, width, fill_char, self.stop_event))

    def __enter__(self):
        self.displayer.start()
        sys.stdout = self.dummy_stdout

    def __exit__(self, exc_type, exc_value, tb):
        self.stop_event.set()
        self.displayer.join()
        sys.stdout = self.original_stdout
        self.dummy_stdout.close()


def loadingbar(msg='Loading', interval=.5, width=7, fill_char='.'):
    """This function creates a context manager that when entered, begins
    displaying an animated loading bar while some code runs.
    The loading bar will be dismissed when 'with-block' for the context
    manager exits.

    Example usage::

        with loadingbar():
            some_indeterminite_length_task()

        with loadingbar(msg='Downloading', width=9, interval=.25, fill_char='#'):
            some_indeterminite_length_task()

    :param msg: The text to display near the bar.
    :param interval: The interval in seconds at which the animation will update.
    :param width: The width in characters of the loading animation.
    :param fill_char: The character to use in the loading bar.
    """
    return LoadingBar(msg=msg, interval=interval, width=width, fill_char=fill_char)
