'''
target-typo-proxy main module
'''
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
# !/usr/bin/env python3
# 11, 20, 29

import argparse
from collections import defaultdict
import io
import sys
import json
import threading
import http.client
import numbers
import urllib
import collections
import pkg_resources
from jsonschema.exceptions import ValidationError, SchemaError
from jsonschema.validators import Draft4Validator

from target_typo_proxy.logging import log_critical, log_debug, log_info
from target_typo_proxy.typo import TargetTypoProxy


# Message types
TYPE_RECORD = 'RECORD'
TYPE_STATE = 'STATE'
TYPE_SCHEMA = 'SCHEMA'


def flatten(data_json, parent_key='', sep='__'):
    '''
    Flattening JSON nested file
    *Singer default template function
    '''
    items = []
    for json_object, json_value in data_json.items():
        new_key = parent_key + sep + json_object if parent_key else json_object
        if isinstance(json_value, collections.MutableMapping):
            items.extend(flatten(json_value, new_key, sep=sep).items())
        else:
            items.append((new_key, str(json_value) if isinstance(json_value, list) else json_value))
    return dict(items)


# pylint: disable=too-many-statements,too-many-branches
def process_lines(config, records):
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
    for record in records:
        try:
            input_record = json.loads(record)
        except json.decoder.JSONDecodeError:
            log_critical('Unable to parse record: %s', record)
            sys.exit(1)

        if 'type' not in input_record:
            log_critical('Line is missing required key "type": %s', record)
            sys.exit(1)

        input_type = input_record['type']

        if input_type == TYPE_RECORD:
            # Validate record
            if input_record['stream'] in validators:
                try:
                    validators[input_record['stream']].validate(input_record['record'])
                except ValidationError as err:
                    log_critical(err)
                    sys.exit(1)
                except SchemaError as err:
                    log_critical('Invalid schema: %s', err)

            processed_records[input_record['stream']] += 1

            flattened_record = flatten(input_record['record'])

            typo.queue_for_processing(
                dataset=input_record['stream'],
                line=flattened_record,
                original_record=record
            )

        elif input_type == TYPE_STATE:
            # Maintain the order of the output messages, clear the current queue
            # before continuing
            if len(typo.data_out) > 0:
                typo.process_batch()
            typo.output_to_all_targets(record)

        elif input_type == TYPE_SCHEMA:
            if 'stream' not in input_record:
                log_critical('Line is missing required key "stream": %s', record)
                sys.exit(1)

            # Maintain the order of the output messages, clear the current queue
            # before continuing
            if len(typo.data_out) > 0:
                typo.process_batch()

            stream = input_record['stream']

            # Validate if stream is processed
            if stream in processed_records.keys():
                log_critical('SCHEMA message should arrive before any RECORD messages.')
                sys.exit(1)

            # Validate schema
            try:
                schemas[stream] = input_record['schema']
            except KeyError:
                log_critical('Missing schema value in SCHEMA record: %s', input_record)
                sys.exit(1)

            validators[stream] = Draft4Validator(input_record['schema'])

            # SCHEMA record is sent to all targets
            typo.output_to_all_targets(record)
        else:
            # UNKNOWN record type
            # Maintain the order of the output messages, clear the current queue
            # before continuing
            if len(typo.data_out) > 0:
                typo.process_batch()
            typo.output_to_all_targets(record)

    if len(typo.data_out) != 0:
        typo.process_batch()

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

    if 'send_threshold' not in config:
        missing_parameters.append('send_threshold')
    else:
        if not isinstance(config['send_threshold'], numbers.Number):
            log_critical('Configuration file parameter "send_threshold" must be a number.')
            return False

        if config['send_threshold'] < 1:
            log_critical('Configuration file parameter "send_threshold" must be higher than 1.')
            return False

        if config['send_threshold'] > 100:
            log_critical('Configuration file parameter "send_threshold" must be lower than 100.')
            return False

    if 'errors_target' not in config and 'valid_target' not in config:
        log_critical('Please provide at least an error or valid target on the'
                     'configuration file "%s" by adding errors_target and/or '
                     'valid_target parameters.', config_loc)
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
