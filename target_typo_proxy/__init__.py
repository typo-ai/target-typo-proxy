# Copyright 2019 Typo. All Rights Reserved.
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
import io
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections
import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

from target_typo_proxy.typo import TargetTypoProxy


logger = singer.get_logger()

# logger.setLevel(logging.DEBUG)

# Type Constants
TYPE_RECORD = 'RECORD'
TYPE_STATE = 'STATE'
TYPE_SCHEMA = 'SCHEMA'


def emit_state(state, typo):
    logger.debug('emit_state - state=[%s]', state)

    if state is not None:
        logger.debug('emit_stat - Emitting state %s', json.dumps(state))
        sys.stdout.write('{}\n'.format(singer.format_message(singer.StateMessage(value=state))))
        sys.stdout.flush()

def flatten(data_json, parent_key='', sep='__'):
    '''
    Flattening JSON nested file
    *Singer default template function
    '''
    logger.debug('flatten - data_json=[%s], parent_key=[%s], sep=[%s]', data_json, parent_key, sep)
    items = []
    for json_object, json_value in data_json.items():
        new_key = parent_key + sep + json_object if parent_key else json_object
        if isinstance(json_value, collections.MutableMapping):
            items.extend(flatten(json_value, new_key, sep=sep).items())
        else:
            items.append((new_key, str(json_value) if type(
                json_value) is list else json_value))
    return dict(items)


def process_lines(config, records):
    logger.debug('process_lines - config=[%s], records=[%s]', config, records)
    state = {'value': {}}
    schemas = {}
    key_properties = {}
    validators = {}
    proxied_key_properties = []
    processed_streams = set()

    now = datetime.now().strftime('%Y%m%dT%H%M%S')  # noqa

    def get_config_val(config, key):
        val = config.get(key)
        if type(val) == str:
            return val.strip()
        return val

    # Typo Proxy Class
    typo = TargetTypoProxy(
        api_key=config['api_key'],
        api_secret=config['api_secret'],
        cluster_api_endpoint=config['cluster_api_endpoint'],
        repository=config['repository'],
        send_threshold=config['send_threshold'],
        errors_target=get_config_val(config, 'errors_target'),
        valid_target=get_config_val(config, 'valid_target'),
        passthrough_target=get_config_val(config, 'passthrough_target')
    )

    typo.token = typo.request_token()

    # Loop over records from stdin
    for record in records:
        try:
            input_record = json.loads(record)
        except json.decoder.JSONDecodeError:
            logger.warn(f'Unable to parse: {record.rstrip()}')
            continue

        if 'type' not in input_record:
            raise Exception(
                'Line is missing required key "type": {}'.format(record))
        input_type = input_record['type']

        if input_type == TYPE_RECORD:
            # Validate record
            if input_record['stream'] in validators:
                try:
                    validators[input_record['stream']].validate(
                        input_record['record'])
                except Exception as err:
                    logger.error(err)
                    sys.exit(1)

            flattened_record = flatten(input_record['record'])

            typo.queue_for_processing(
                dataset=input_record['stream'],
                line=flattened_record,
                original_record=record
            )

            # Outputting state for proxied key_properties
            if input_record['stream'] in key_properties:
                if len(key_properties[input_record['stream']]) != 0:
                    key_json = {}
                    for key in key_properties[input_record['stream']]:
                        key_json[key] = input_record['record'][key]
                    proxied_key_properties.append(key_json)

            # Adding processed streams
            processed_streams.add(input_record['stream'])

        elif input_type == TYPE_STATE:
            # Maintain the order of the output messages, clear the current queue
            # before continuing
            if len(typo.data_out) > 0:
                typo.process_batch()
            typo.output_to_all_targets(record)

        elif input_type == TYPE_SCHEMA:
            if 'stream' not in input_record:
                raise Exception(
                    'Line is missing required key "stream": {}'.format(record))

            # Maintain the order of the output messages, clear the current queue
            # before continuing
            if len(typo.data_out) > 0:
                typo.process_batch()

            stream = input_record['stream']

            # Validate if stream is processed
            if stream in processed_streams:
                logger.error(
                    'Tap error. SCHEMA record should be specified before \
                        RECORDS.')
                sys.exit(1)

            # Validate schema
            try:
                schemas[stream] = input_record['schema']
            except Exception:
                logger.error('Tap error: Schema is missing.')
                sys.exit(1)

            validators[stream] = Draft4Validator(input_record['schema'])

            if 'key_properties' not in input_record:
                raise Exception('key_properties field is required')

            key_properties[stream] = input_record['key_properties']

            # SCHEMA record is sent to all targets
            typo.output_to_all_targets(record)
        else:
            raise Exception('Unknown message type {} in message {}'
                            .format(input_record['type'], input_record))

    if len(typo.data_out) != 0:
        typo.process_batch()

    logger.info(
        'Target Typo Proxy processing completed. {} records proxied.'.format(
            len(proxied_key_properties)))
    state['typo_proxied_records'] = proxied_key_properties
    emit_state(state, typo)


def send_usage_stats():
    logger.debug('send_usage_stats')
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
    except Exception:
        logger.debug('Collection request failed', exc_info=True)
    finally:
        if conn:
            conn.close()


def validate_config(config, config_loc):
    logger.debug('validate_config - config=[%s], config_loc=[%s]', config, config_loc)
    logger.info('Input Configuration Parameters: {}'.format(config))

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

    if 'errors_target' not in config and 'valid_target' not in config:
        logger.error('Please provide at least an error or valid target on the'
                     'configuration file "{0}" by adding errors_target and/or '
                     'valid_target parameters.'.format(config_loc))
        return False

    # Output error message if there are missing parameters
    if len(missing_parameters) != 0:
        sep = ','
        logger.error('Configuration parameter missing. Please',
                     'set the [{0}] parameter in the configuration file "{1}"'.format(
                         sep.join(missing_parameters, config_loc)))
        return False

    return True


def main():
    logger.debug('main')
    logger.info('\'target-typo-proxy:{}\' Starting...'.format(
        pkg_resources.get_distribution('target_typo_proxy').version))
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
            logger.info(
                'Target configuration file {} loaded.'.format(args.config))
    else:
        logger.error('Please specify configuration file.')
        sys.exit(1)

    # Validate configuration for required parameters
    config_validation = validate_config(config, args.config)

    if not config_validation:
        logger.info('Configuration errors found. Target exiting.')
        return

    if not config.get('disable_collection', False):
        logger.info(
            'Sending version information to singer.io.',
            'To disable sending anonymous usage data, set',
            'the config parameter "disable_collection" to true')

        threading.Thread(target=send_usage_stats).start()

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    process_lines(config, input)

    logger.info('Target exiting normally')


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        logger.error('Target-typo cannot be executed at the moment. \
            Please try again later. Details: {}'.format(err))
        sys.exit(1)
