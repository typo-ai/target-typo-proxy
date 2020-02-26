#!/usr/bin/env python3
# Copyright 2019-2020 Typo. All Rights Reserved.
#
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
#
# you may not use this file except in compliance with the
#
# License.
#
#
#
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
#
#
# Unless required by applicable law or agreed to in writing, software
#
# distributed under the License is distributed on an "AS IS" BASIS,
#
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#
# implied. See the License for the specific language governing
#
# permissions and limitations under the License.
#
#
#
# This product includes software developed at
#
# or by Typo (https://www.typo.ai/).

import argparse
from collections import defaultdict
import io
import sys
import json
import threading
import http.client
import numbers
import urllib
import pkg_resources

from jsonschema.exceptions import ValidationError, SchemaError
from jsonschema.validators import Draft4Validator

from target_typo_proxy.constants import TYPE_RECORD, TYPE_SCHEMA, TYPE_STATE
from target_typo_proxy.logging import log_critical, log_debug, log_info
from target_typo_proxy.typo import TargetTypoProxy


# pylint: disable=too-many-statements,too-many-branches
def process_lines(config, messages):
    '''
    Loops through stdin input and processes each message
    '''
    schemas = {}
    validators = {}
    processed_records = defaultdict(lambda: 0)

    # Typo Proxy Class
    typo = TargetTypoProxy(config)

    typo.token = typo.request_token()

    # Loop over records from stdin
    for raw_message in messages:
        try:
            message = json.loads(raw_message)
        except json.decoder.JSONDecodeError:
            log_critical('Unable to parse message: %s', raw_message)
            sys.exit(1)

        if 'type' not in message:
            log_critical('Line is missing required key "type": %s', message)
            sys.exit(1)

        message_type = message['type']

        if message_type == TYPE_RECORD:
            # Validate Record
            if message['stream'] in validators:
                try:
                    validators[message['stream']].validate(message['record'])
                except ValidationError as err:
                    log_critical(err)
                    sys.exit(1)
                except SchemaError as err:
                    log_critical('Invalid schema: %s', err)
                    sys.exit(1)

        elif message_type == TYPE_SCHEMA:
            # Validate Schema
            if 'stream' not in message:
                log_critical('Line is missing required key "stream": %s', raw_message)
                sys.exit(1)

            stream = message['stream']

            # Validate if stream is processed
            if stream in processed_records.keys():
                log_critical('SCHEMA message should arrive before any RECORD messages.')
                sys.exit(1)

            try:
                schemas[stream] = message['schema']
            except KeyError:
                log_critical('Missing schema value in SCHEMA record: %s', message)
                sys.exit(1)

            validators[stream] = Draft4Validator(message['schema'])

        # Enqueue every message for processing
        typo.enqueue_message({
            'raw_message': raw_message,
            'message': message
        })

    if len(typo.message_queue) != 0:
        typo.process_buffer()

    log_info('target-typo-proxy no longer receiving input. Processing completed.')

    for stream_name, record_count in processed_records.items():
        log_info('Processed stream %s with %s records.', stream_name, record_count)


def send_usage_stats():
    '''
    Sends usage stats to Singer.io
    '''
    try:
        version = pkg_resources.get_distribution('target-typo-proxy').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-typo-proxy',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        conn.getresponse()

    # pylint: disable=W0703
    except Exception:
        log_debug('Collection request failed', exc_info=True)

    finally:
        if conn:
            conn.close()


def validate_number_value(parameter_name, value, min_value, max_value):
    if not isinstance(value, numbers.Number):
        log_critical('Configuration file parameter "{}" must be a number.'.format(parameter_name))
        return False

    if value < min_value:
        log_critical('Configuration file parameter "{}" must be higher than {}.'.format(parameter_name, min_value))
        return False

    if value > max_value:
        log_critical('Configuration file parameter "{}" must be lower than or equal to {}.'.format(parameter_name, max_value))
        return False

    return True


def validate_config(config, config_loc):
    '''
    Validates the provided configuration file
    '''
    missing_parameters = []

    if 'api_key' not in config:
        missing_parameters.append('api_key')

    if 'api_secret' not in config:
        missing_parameters.append('api_secret')

    if 'cluster_api_endpoint' not in config:
        missing_parameters.append('cluster_api_endpoint')

    if 'repository' not in config:
        missing_parameters.append('repository')

    # Optional parameters
    if 'send_threshold' in config:
        if not validate_number_value('send_threshold', config['send_threshold'], 1, 100):
            return False

    if 'errors_target' not in config and 'valid_target' not in config:
        log_critical('Please provide at least an error or valid target on the'
                     'configuration file "%s" by adding errors_target and/or '
                     'valid_target parameters.', config_loc)
        return False

    if 'record_timeout' in config:
        if not validate_number_value('send_threshold', config['send_threshold'], 0, 100):
            return False

    if 'fail_on_partial_results' in config:
        if not isinstance(config['fail_on_partial_results'], bool):
            log_critical('fail_on_partial_results must be a boolean value')
            return False

    if 'record_timeout' in config:
        if not validate_number_value('record_timeout', config['record_timeout'], 0, 10):
            return False

    # Output error message if there are missing parameters
    if len(missing_parameters) != 0:
        sep = ','
        log_critical('Configuration parameter missing. Please set the [%s] parameter%s in the configuration file.',
                     sep.join(missing_parameters), 's' if len(missing_parameters) > 1 else '')
        return False

    return True


def main():
    '''
    Execution starts here
    '''
    log_info('Starting...')
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as config_file:
            config = json.load(config_file)
            log_info('Configuration file {} loaded.'.format(args.config))
    else:
        log_critical('Please specify configuration file.')
        sys.exit(1)

    # Validate configuration for required parameters
    config_validation = validate_config(config, args.config)

    if not config_validation:
        sys.exit(1)

    if not config.get('disable_collection', False):
        log_info('Sending version information to singer.io. To disable sending anonymous usage data, set' +
                 'the config parameter "disable_collection" to true')

        threading.Thread(target=send_usage_stats).start()

    stdin_input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    process_lines(config, stdin_input)

    log_info('Exiting normally.')


if __name__ == '__main__':
    try:
        main()
    # pylint: disable=W0703
    except Exception as err:
        log_critical('Target-typo cannot be executed at the moment. ' +
                     'Please try again later. Details: %s', err)
        sys.exit(1)
