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


import sys
import json
from queue import Queue, Empty
import requests
import shlex
import subprocess
from threading import Thread
from urllib.parse import urlparse

import singer

# Singer Logger
logger = singer.get_logger()

ERROR = 'ERROR'
PASSTHROUGH = 'PASSTHROUGH'
VALID = 'VALID'

TARGETS = [PASSTHROUGH, ERROR, VALID]


def enqueue_output(out, queue):
    '''
    Receives stderr output from subprocess and stores
    it temporarily in a queue
    '''
    for line in iter(out.readline, b''):
        queue.put(line.decode('utf-8'))
    out.close()


class TargetTypoProxy():
    '''
    TypoTargetProxy Module Constructor
    '''
    def __init__(self, api_key, api_secret, cluster_api_endpoint, repository,
                 send_threshold, errors_target, valid_target, passthrough_target):
        logger.debug('__init__ - self=[%s], api_key=[%s], api_secret=[%s], cluster_api_endpoint=[%s],\
                     repository=[%s], send_threshold=[%s]',
                     self, api_key, api_secret, cluster_api_endpoint, repository, send_threshold)

        endpoint_url_parts = urlparse(cluster_api_endpoint)

        self.base_url = cluster_api_endpoint
        self.cluster_url = '{}://{}'.format(endpoint_url_parts.scheme, endpoint_url_parts.netloc)
        self.api_key = api_key
        self.api_secret = api_secret
        self.repository = repository
        self.retry_bool = False
        self.token = ''
        self.data_out = []
        self.schema_records = {}
        self.terminating_subprocesses = False
        self.current_dataset = None

        self.send_threshold = int(send_threshold)

        def is_none_or_empty(val):
            return val is None or val == ''

        if is_none_or_empty(errors_target) and is_none_or_empty(valid_target) and is_none_or_empty(passthrough_target):
            raise Exception("You must specify at least an errors_target, valid_target or passthrough_target through configuration.")

        self.output_targets = {
            ERROR: errors_target,
            VALID: valid_target,
            PASSTHROUGH: passthrough_target
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
        if self.terminating_subprocesses:
            # Don't output any more data if a subprocess has terminated
            return

        if self.output_subprocesses[target_name] is None:
            # Start subprocess if not yet started
            self.output_subprocesses[target_name] = subprocess.Popen(
                shlex.split(self.output_targets[target_name]),
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
        for target_name in TARGETS:
            if self.output_targets[target_name]:
                self.output_to_subprocess_target(target_name, data)

    def terminate_subprocess(self, target_name):
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
        if self.terminating_subprocesses:
            return

        self.terminating_subprocesses = True

        for target_name, value in self.output_targets.items():
            if value:
                self.terminate_subprocess(target_name)

    def subprocess_error_exit(self, target_name):
        self.terminate_subprocesses()
        error_output = 'An error occurred in target "{}":\n'.format(
            self.output_targets[target_name])

        # Get errors from subprocess STDOUT
        if self.stderr_monitoring_queues[target_name]:
            a = 0

            while True:
                try:
                    error_output += self.stderr_monitoring_queues[target_name].get_nowait()
                except Empty:
                    break

                a += 1

        error_output += 'Terminating target-typo-proxy'
        logger.error(error_output)

        # Exit with error
        exit(1)

    def post_request(self, url, headers, payload):
        '''
        Generic POST request
        '''

        logger.debug('post_request - self=[%s], url=[%s], headers=[%s], payload=[%s]',
                     self, url, headers, payload)

        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload))
            logger.debug('post_request - r.text=[%s], data=[%s]', r.text, json.dumps(payload))
        except Exception as e:
            logger.error('post_request - Request failed.')
            logger.error(e)
            sys.exit(1)

        logger.debug('post_request - url=[%s], request.status_code=[%s]', url, r.status_code)
        status = r.status_code
        if status == 200:
            data = r.json()
            return status, data
        else:
            logger.error('post_request - url=[%s], request.status_code=[%s], response.text=[%s]',
                         url, r.status_code, r.text)
            raise Exception('url {} returned status code {}. Please \
                            check that you are using the correct url.'.format(url, r.status_code))

    def request_token(self):
        '''
        Token Request for other requests
        '''
        logger.debug('request_token - self=[%s]', self)

        # Required parameters
        url = self.base_url.rstrip('/') + '/token'
        headers = {
            'Content-Type': 'application/json'
            # ,'Authorization': 'Bearer {}'.format(self.access_token)
        }
        payload = {
            'apikey': self.api_key,
            'secret': self.api_secret
        }

        # POST request
        try:
            status, data = self.post_request(url, headers, payload)
        except Exception:
            logger.error('request_token - Please validate your configuration inputs.',
                         exc_info=True)
            sys.exit(1)

        # Check Status
        if status != 200:
            logger.error(
                'request_token - Token Request Failed. Please check your credentials. Details: \
                {}'.format(data))
            sys.exit(1)

        return data['token']

    def queue_for_processing(self, dataset, line, original_record):
        '''
        Constructing record data payload for POST Request
        '''
        logger.debug('queue_for_processing - self=[%s], dataset=[%s], line=[%s]',
                     self, dataset, line)

        data = {
            'typo_data': {
                'apikey': self.repository,
                'url': dataset,
                'data': line
            },
            'original_record': original_record
        }

        # Submit a batch once we reach the record threshold or if dataset changes
        # The previous batch is actually submitted and the current record is added
        # to the next batch (new)
        if ((len(self.data_out) == self.send_threshold)
                or (self.current_dataset and self.current_dataset != dataset)):
            self.current_dataset = dataset
            self.process_batch()

        self.data_out.append(data)

        if not self.current_dataset:
            self.current_dataset = dataset

        return data

    def process_batch(self):
        '''
        Validate data with Typo in batch via POST Request to /predict-batch and push to
        corresponding output target.
        '''
        if len(self.data_out) == 0:
            return

        batch = self.data_out
        self.data_out = []

        # Required parameters
        url = self.cluster_url + '/predict-batch'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.token
        }

        post_data = {
            'apikey': batch[0]['typo_data']['apikey'],
            'url': batch[0]['typo_data']['url'],
            'data': [record['typo_data']['data'] for record in batch]
        }

        logger.debug('process_batch - POST records: {}'.format(batch))
        status, data = self.post_request(url, headers, post_data)

        # Expired token
        if status == 401:
            logger.debug('process_data - Token expired. Requesting new token.')
            self.token = self.request_token()

            # Retry post_request with new token
            status, data = self.post_request(url, headers, post_data)

        # Check Status
        good_status = [200, 201, 202]
        if status not in good_status:
            logger.error(
                'process_data - Request failed. Please try again later. {}\
                    '.format(data['message']))
            sys.exit(1)

        for index, result in enumerate(data['data']):
            if self.output_targets[PASSTHROUGH]:
                self.output_to_subprocess_target(PASSTHROUGH, batch[index]['original_record'])

            if result['status'] == 'OK' and self.output_targets[VALID]:
                self.output_to_subprocess_target(VALID, batch[index]['original_record'])
            elif self.output_targets[ERROR]:
                self.output_to_subprocess_target(ERROR, batch[index]['original_record'])
