# Copyright 2019-2020 Typo. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at or by Typo (https://www.typo.ai/).

import sys
import pkg_resources
import singer

# Singer Logger
LOGGER = singer.get_logger()


def format_log_message(message, new_line):
    '''
    Adds target-typo-proxy label and version to log messages
    '''
    return '\'target-typo-proxy:{}\'{}{}'.format(
        pkg_resources.get_distribution('target_typo_proxy').version,
        '\n' if new_line else ' ',
        message
    )


def log_debug(message, *args, exc_info=False, new_line=False, **kwargs):
    '''
    Logs a debug message
    '''
    LOGGER.debug(format_log_message(message, new_line), exc_info=exc_info, *args, **kwargs)


def log_error(message, *args, exc_info=False, new_line=False, **kwargs):
    '''
    Logs a normal error
    '''
    LOGGER.error(format_log_message(message, new_line), exc_info=exc_info, *args, **kwargs)


def log_info(message, *args, exc_info=False, new_line=False, **kwargs):
    '''
    Logs an info message
    '''
    LOGGER.info(format_log_message(message, new_line), exc_info=exc_info, *args, **kwargs)


def log_critical(message, *args, exc_info=False, new_line=False, **kwargs):
    '''
    Logs a critical error
    '''
    LOGGER.critical(format_log_message(message, new_line), exc_info=exc_info, *args, **kwargs)


def log_backoff(details):
    '''
    Logs a backoff retry message
    '''
    (_, exc, _) = sys.exc_info()

    log_info(
        'Network error receiving data from Typo. Sleeping {:.1f} seconds before trying again: {}'.format(
            details['wait'], exc))
