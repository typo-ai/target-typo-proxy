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


from collections import defaultdict, deque
import json
import os
from queue import Queue, Empty
import shlex
import subprocess
import sys
from threading import Thread
from urllib.parse import urlparse

import backoff
import requests

from target_typo_proxy.constants import TYPE_RECORD
from target_typo_proxy.logging import log_backoff, log_critical, log_debug, log_info
from target_typo_proxy.utils import flatten


ERROR = 'ERROR'
PASSTHROUGH = 'PASSTHROUGH'
VALID = 'VALID'

TARGETS = [PASSTHROUGH, ERROR, VALID]

IS_POSIX = os.name == 'posix'


def enqueue_output(out, queue):
    '''
    Receives stderr output from subprocess and stores
    it temporarily in a queue
    '''
    for line in iter(out.readline, b''):
        queue.put(line.decode('utf-8'))
    out.close()


# pylint: disable=unused-argument
def backoff_giveup(exception):
    '''
    Called when backoff exhausts max tries
    '''
    log_critical('Unable to make network requests. Please check your internet connection.')
    sys.exit(1)


class TargetTypoProxy():
    '''
    TypoTargetProxy Module Constructor
    '''
    # pylint: disable=too-many-instance-attributes

    def __init__(self, config):
        self.config = config

        self.api_key = config['api_key']
        self.api_secret = config['api_secret']

        self.base_url = config['cluster_api_endpoint']
        endpoint_url_parts = urlparse(self.base_url)
        self.cluster_url = '{}://{}'.format(endpoint_url_parts.scheme, endpoint_url_parts.netloc)
        self.repository = config['repository']
        self.send_threshold = config['send_threshold'] if 'send_threshold' in config else 100
        self.record_timeout = config['record_timeout'] if 'record_timeout' in config else None
        self.fail_on_partial_results = (
            config['fail_on_partial_results'] if 'fail_on_partial_results' in config else True)
        self.retry_times = config['retry_times'] if 'retry_times' in config else None

        self.retry_bool = False
        self.token = ''
        self.data_out = []
        self.schema_records = {}
        self.terminating_subprocesses = False
        self.current_dataset = None

        self.send_threshold = int(config['send_threshold'])
        self.batch_number = 0

        self.record_buffer_sizes = defaultdict(lambda: 0)
        self.message_queue = []
        self.validators = {}

        self.current_stream = None

        def is_none_or_empty(val):
            return val is None or val == ''

        errors_target = config.get('errors_target', None)
        valid_target = config.get('valid_target', None)
        passthrough_target = config.get('passthrough_target', None)

        if (is_none_or_empty(errors_target) and is_none_or_empty(valid_target)
                and is_none_or_empty(passthrough_target)):

            raise Exception('You must specify at least an errors_target, valid_target or passthrough_target ' +
                            'through configuration.')

        self.output_targets = {
            ERROR: config.get('errors_target', None),
            VALID: config.get('valid_target', None),
            PASSTHROUGH: config.get('passthrough_target', None)
        }

        self.output_subprocesses = {
            ERROR: None,
            VALID: None,
            PASSTHROUGH: None
        }

        self.stderr_monitoring_threads = {
            ERROR: None,
            VALID: None,
            PASSTHROUGH: None
        }

        self.stderr_monitoring_queues = {
            ERROR: None,
            VALID: None,
            PASSTHROUGH: None
        }

    def output_to_subprocess_target(self, target_name, data):
        '''
        Outputs messages to the indicated subprocess target
        '''
        if self.terminating_subprocesses:
            # Don't output any more data if a subprocess has terminated
            return

        if self.output_subprocesses[target_name] is None:
            # Start subprocess if not yet started
            log_info('Starting %s target subprocess: "%s"', target_name, self.output_targets[target_name])
            self.output_subprocesses[target_name] = subprocess.Popen(
                shlex.split(self.output_targets[target_name], posix=IS_POSIX),
                shell=False,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # A new thread is started for monitoring subprocess stderr output
            # because stderr readline would block the main thread
            self.stderr_monitoring_queues[target_name] = Queue()

            self.stderr_monitoring_threads[target_name] = Thread(
                target=enqueue_output,
                args=(
                    self.output_subprocesses[target_name].stderr,
                    self.stderr_monitoring_queues[target_name]
                )
            )
            self.stderr_monitoring_threads[target_name].daemon = True
            self.stderr_monitoring_threads[target_name].start()

        try:
            self.output_subprocesses[target_name].stdin.write((data).encode('utf-8'))
            # Prevent subprocess input buffering
            self.output_subprocesses[target_name].stdin.flush()

        except BrokenPipeError:
            # Subprocess has exited unexpectedly
            self.subprocess_error_exit(target_name)

    def output_to_all_targets(self, data):
        '''
        Outputs a message to all available targets
        '''
        for target_name in TARGETS:
            if self.output_targets[target_name]:
                self.output_to_subprocess_target(target_name, data)

    def terminate_subprocess(self, target_name):
        '''
        Terminates or kills a subprocess
        '''
        # When a subprocess fails, the other subprocesses must be terminated
        if self.output_subprocesses[target_name].poll() is None:
            self.output_subprocesses[target_name].stdin.close()
            try:
                self.output_subprocesses[target_name].wait(1)
            except subprocess.TimeoutExpired:
                self.output_subprocesses[target_name].terminate()

                # Wait a maximum of 1 second before killing the process
                try:
                    self.output_subprocesses[target_name].wait(1)
                except subprocess.TimeoutExpired:
                    self.output_subprocesses[target_name].kill()

                    try:
                        self.output_subprocesses[target_name].wait(1)
                    except subprocess.TimeoutExpired:
                        pass

    def terminate_subprocesses(self):
        '''
        Terminates or kills all subprocesses
        '''
        if self.terminating_subprocesses:
            return

        self.terminating_subprocesses = True

        for target_name, value in self.output_targets.items():
            if value:
                self.terminate_subprocess(target_name)

    def subprocess_error_exit(self, target_name):
        '''
        A subprocess has exited generating a BrokenPipeError
        '''
        self.terminate_subprocesses()
        error_output = 'An error occurred in target "{}":\n'.format(
            self.output_targets[target_name])

        # Get errors from subprocess STDOUT
        if self.stderr_monitoring_queues[target_name]:
            while True:
                try:
                    error_output += self.stderr_monitoring_queues[target_name].get_nowait()
                except Empty:
                    break

        error_output += 'Terminating target-typo-proxy.'
        log_critical(error_output)
        sys.exit(1)

    # pylint: disable=no-self-use
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
        max_tries=8,
        on_backoff=log_backoff,
        on_giveup=backoff_giveup,
        logger=None,
        factor=3
    )
    def post_request(self, url, headers, payload):
        '''
        Generic POST request
        '''
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)

        status = response.status_code

        if status == 200:
            data = response.json()
            return status, data

        log_critical('URL %s returned status code %s. Please check that you are using the correct url.',
                     url, response.status_code)
        sys.exit(1)

    def request_token(self):
        '''
        Token Request for other requests
        '''
        # Required parameters
        url = self.base_url.rstrip('/') + '/token'
        headers = {
            'Content-Type': 'application/json'
        }
        payload = {
            'apikey': self.api_key,
            'secret': self.api_secret
        }

        # POST request
        status, data = self.post_request(url, headers, payload)

        # Check Status
        if status != 200:
            log_critical('Token Request Failed. Please check your credentials. Details: %s', data)
            sys.exit(1)

        return data['token']

    def enqueue_message(self, message_data):
        message = message_data['message']

        self.message_queue.append(message_data)

        if message['type'] == TYPE_RECORD:
            self.record_buffer_sizes[message['type']] += 1

            if self.record_buffer_sizes[message['type']] == self.send_threshold:
                self.process_buffer()

    def process_buffer(self):
        '''
        Processes the whole buffer, generates one batch per stream and outputs
        the results to the respective subprocesses.
        '''
        batches = defaultdict(list)
        results = {}

        for message in self.message_queue:
            if message['message']['type'] == TYPE_RECORD:
                batches[message['message']['stream']].append(message)

        for stream, batch in batches.items():
            results[stream] = deque(self.process_batch(stream, batch))

        errors_count = 0
        valid_count = 0

        for message in self.message_queue:
            if message['message']['type'] == TYPE_RECORD:
                result = results[message['message']['stream']].popleft()

                if self.output_targets[PASSTHROUGH]:
                    self.output_to_subprocess_target(PASSTHROUGH, message['raw_message'])

                if result['status'] == 'OK':
                    valid_count += 1
                    if self.output_targets[VALID]:
                        self.output_to_subprocess_target(VALID, message['raw_message'])
                else:
                    errors_count += 1
                    if self.output_targets[ERROR]:
                        self.output_to_subprocess_target(ERROR, message['raw_message'])
            else:
                self.output_to_all_targets(message['raw_message'])

        if errors_count > 0 and self.output_targets[ERROR]:
            log_info('Batch %s: Sending %s records to ERROR target.', self.batch_number, errors_count)

        if valid_count > 0 and self.output_targets[VALID]:
            log_info('Batch %s: Sending %s records to VALID target.', self.batch_number, valid_count)

        if self.output_targets[PASSTHROUGH]:
            log_info('Batch %s: Sending %s records to PASSTHROUGH target.', self.batch_number, valid_count)

        # Reset message queue
        self.message_queue = []

        # Clear the per-stream buffer size counter
        for key in self.record_buffer_sizes.keys():
            self.record_buffer_sizes[key] = 0

    def process_batch(self, stream, batch):
        '''
        Validate data with Typo for one batch, corresponding to a single dataset (stream).
        Executes POST Request to /predict-batch and pushes to corresponding output target.
        '''
        self.batch_number += 1

        log_info('Batch %s: Validating %s records with Typo.', self.batch_number, len(batch))

        # Required parameters
        url = self.cluster_url + '/predict-batch'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.token
        }

        post_data = {
            'apikey': self.repository,
            'url': stream,  # URL (dataset) is stream name
            'data': [flatten(message['message']['record']) for message in batch],
            'fail_on_partial_results': self.fail_on_partial_results
        }

        if self.record_timeout is not None:
            post_data['record_timeout'] = self.record_timeout

        if self.retry_times is not None:
            post_data['retry_times'] = self.retry_times

        status, data = self.post_request(url, headers, post_data)

        # Expired token
        if status == 401:
            log_debug('Token expired. Requesting new token.')
            self.token = self.request_token()

            # Retry post_request with new token
            status, data = self.post_request(url, headers, post_data)

        # Check Status
        good_status = [200, 201, 202]
        if status not in good_status:
            log_critical('Request failed. Please try again later. %s', data['message'])
            sys.exit(1)

        return data['data']
