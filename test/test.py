# Copyright 2019 Typo. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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
#
# !/usr/bin/env python3

import json
import unittest
from unittest.mock import patch, call
import target_typo_proxy.__init__ as init
from target_typo_proxy.typo import TargetTypoProxy, ERROR, VALID, PASSTHROUGH

TEST_ENDPOINT = 'https://www.mock.com'
TEST_TOKEN = 'some-token-12345'
TEST_API_KEY = 'test-api-key-12345'
TEST_API_SECRET = 'test-api-secret-12345'
TEST_REPOSITORY = 'test-repository'
TEST_CONFIG_MISSING_TARGETS = {
    'api_key': TEST_API_SECRET,
    'api_secret': TEST_API_SECRET,
    'cluster_api_endpoint': TEST_ENDPOINT,
    'send_threshold': 5,
    'repository': TEST_REPOSITORY,
    'errors_target': '',
    'valid_target': '',
    'passthrough_target': '',
}

TEST_CONFIG = {
    'api_key': TEST_API_SECRET,
    'api_secret': TEST_API_SECRET,
    'cluster_api_endpoint': TEST_ENDPOINT,
    'send_threshold': 1,
    'repository': TEST_REPOSITORY,
    'errors_target': "/bin/sh -c 'while read -r line; do echo $line; done;'",
    'valid_target': "/bin/sh -c 'while read -r line; do echo $line; done;'",
    'passthrough_target': "/bin/sh -c 'while read -r line; do echo $line; done;'",
}

dataset = "dataset"
data = {
    "date": "2019-06-23",
    "user": "leo"
}


class TestTypoTargetProxy(unittest.TestCase):

    def test_no_target_provided(self):
        print("Test: When no targets are provided the the target should exit with error.")
        with self.assertRaises(Exception):
            init.process_lines(TEST_CONFIG_MISSING_TARGETS, [])

    def test_schema_records_should_be_sent_to_all_targets(self):
        print("Test: Schema records should be sent to all targets.")
        schema_record = json.dumps({
            "type": "SCHEMA",
            "stream": "mock",
            "schema": {},
            "key_properties": ["date"]
        })
        records = [schema_record]
        with patch('target_typo_proxy.typo.TargetTypoProxy.request_token') as mocked_request_token:
            mocked_request_token.return_value = TEST_TOKEN
            with patch('target_typo_proxy.typo.TargetTypoProxy.output_to_all_targets') as mocked_output_to_all_targets:
                init.process_lines(TEST_CONFIG, records)
                mocked_output_to_all_targets.called_with(schema_record)

    def test_state_records_should_be_sent_to_all_targets(self):
        print("Test: Schema records should be sent to all targets.")
        state_record = json.dumps({
            "type": "STATE",
            "value": {
                "start_date": "today"
            }
        })
        records = [state_record]
        with patch('target_typo_proxy.typo.TargetTypoProxy.request_token') as mocked_request_token:
            mocked_request_token.return_value = TEST_TOKEN
            with patch('target_typo_proxy.typo.TargetTypoProxy.output_to_all_targets') as mocked_output_to_all_targets:
                init.process_lines(TEST_CONFIG, records)
                mocked_output_to_all_targets.called_with(state_record)

    def test_records_should_be_sent_to_the_predict_endpoint(self):
        print("Test: Records should be sent to predict endpoint")

        # Mock the request_token, mock the predict endpoint
        with patch('target_typo_proxy.typo.TargetTypoProxy.request_token') as mocked_request_token:
            mocked_request_token.return_value = TEST_TOKEN
            with patch('target_typo_proxy.typo.requests.post') as mocked_post:
                mocked_post.return_value.status_code = 200

                record_data = {
                    "date": "today",
                    "subj": "mock"
                }
                mocked_post.return_value.json.return_value = {
                    'data': [{'status': 'OK', 'record': record_data}]}
                schema_record = json.dumps({
                    "type": "SCHEMA",
                    "stream": "mock",
                    "schema": {},
                    "key_properties": ["date"]
                })

                record = json.dumps({
                    "type": "RECORD",
                            "stream": "mock",
                            "record": record_data
                })

                expected_url = TEST_ENDPOINT + '/predict-batch'
                expected_headers = {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + TEST_TOKEN
                }
                expected_payload = {
                    "apikey": TEST_REPOSITORY,
                    "url": "mock",
                    "data": [
                        record_data
                    ]
                }

                records = [schema_record, record]
                init.process_lines(TEST_CONFIG, records)
                mocked_post.assert_called_with(expected_url,
                                               data=json.dumps(expected_payload),
                                               headers=expected_headers)

    def test_error_records_should_be_sent_to_errors_target(self):
        print("Test: ERROR records should be sent to errors_target")

        # Mock the request_token, mock the predict endpoint
        with patch('target_typo_proxy.typo.TargetTypoProxy.request_token') as mocked_request_token:
            mocked_request_token.return_value = TEST_TOKEN
            with patch('target_typo_proxy.typo.requests.post') as mocked_post:
                mocked_post.return_value.status_code = 200

                record_data = {
                    "date": "today",
                    "subj": "mock"
                }
                mocked_post.return_value.json.return_value = {
                    'data': [{'status': 'ERROR', 'record': record_data}]}
                schema_record = json.dumps({
                    "type": "SCHEMA",
                    "stream": "mock",
                    "schema": {},
                    "key_properties": ["date"]
                })

                record = json.dumps({
                    "type": "RECORD",
                            "stream": "mock",
                            "record": record_data
                })

                records = [schema_record, record]
                init.process_lines(TEST_CONFIG, records)
                with patch('target_typo_proxy.typo.TargetTypoProxy.output_to_subprocess_target') as mocked_output_to_subprocess:
                    init.process_lines(TEST_CONFIG, records)

                expected_calls = [call(PASSTHROUGH, schema_record), call(ERROR, schema_record), call(VALID, schema_record), call(PASSTHROUGH, record), call(ERROR, record)]
                mocked_output_to_subprocess.assert_has_calls(expected_calls)

    def test_ok_records_should_be_sent_to_valid_target(self):
        print("Test: OK records should be sent to valid_target")

        # Mock the request_token, mock the predict endpoint
        with patch('target_typo_proxy.typo.TargetTypoProxy.request_token') as mocked_request_token:
            mocked_request_token.return_value = TEST_TOKEN
            with patch('target_typo_proxy.typo.requests.post') as mocked_post:
                mocked_post.return_value.status_code = 200

                record_data = {
                    "date": "today",
                    "subj": "mock"
                }
                mocked_post.return_value.json.return_value = {
                    'data': [{'status': 'OK', 'record': record_data}]}
                schema_record = json.dumps({
                    "type": "SCHEMA",
                    "stream": "mock",
                    "schema": {},
                    "key_properties": ["date"]
                })

                record = json.dumps({
                    "type": "RECORD",
                            "stream": "mock",
                            "record": record_data
                })

                records = [schema_record, record]
                init.process_lines(TEST_CONFIG, records)
                with patch('target_typo_proxy.typo.TargetTypoProxy.output_to_subprocess_target') as mocked_output_to_subprocess:
                    init.process_lines(TEST_CONFIG, records)

                expected_calls = [call(PASSTHROUGH, schema_record), call(ERROR, schema_record), call(VALID, schema_record), call(PASSTHROUGH, record), call(VALID, record)]
                mocked_output_to_subprocess.assert_has_calls(expected_calls)

    def test_request_token(self):
        print("Test: When API key and API secret are provided, a token",
              "property should be returned by calling the token endpoint.")

        typo_target_proxy = TargetTypoProxy(
            cluster_api_endpoint=TEST_ENDPOINT,
            api_key=TEST_API_KEY,
            api_secret=TEST_API_SECRET,
            repository='test_typo',
            send_threshold=5,
            errors_target='blah',
            valid_target='blah',
            passthrough_target='blah'
        )

        with patch('target_typo_proxy.typo.requests.post') as mocked_post:
            mocked_post.return_value.status_code = 200
            mocked_post.return_value.json.return_value = {"token": TEST_TOKEN}

            expected_headers = {
                "Content-Type": "application/json"
            }
            expected_payload = {
                "apikey": TEST_API_KEY,
                "secret": TEST_API_SECRET
            }

            token = typo_target_proxy.request_token()
            mocked_post.assert_called_with(TEST_ENDPOINT + '/token',
                                           data=json.dumps(expected_payload),
                                           headers=expected_headers)
            self.assertEqual(token, TEST_TOKEN)

if __name__ == '__main__':
    unittest.main()
