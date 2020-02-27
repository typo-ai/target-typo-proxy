# target-typo-proxy

[Singer](https://singer.io) target that intercepts data traveling between a tap and target in order to provide data quality checking with [Typo](https://www.typo.ai/?utm_source=github&utm_medium=target-typo-proxy). Records received will be forwarded to Typo, and depending on the results returned, **target-typo-proxy** forwards the record to a valid or error target. An optional passthrough target receives all records regardless of Typo results.

- [Usage](#usage)
  - [Installation](#installation)
  - [Create a configuration file](#create-a-configuration-file)
  - [Run target-typo-proxy](#run-target-typo-proxy)
  - [Saving state](#saving-state)
- [Typo registration and setup](#typo-registration-and-setup)
- [Development](#development)
- [Support](#support)



## Usage

This section describes the basic usage of **target-typo-proxy**. It assumes that you already have a Typo account, with an existing repository and a dataset. If you do not meet these prerequisites, please go to [Typo Registration and Setup](#typo-registration-and-setup).



### Installation

Python 3 is required. It is recommended to create a separate virtual environment for each tap or target as their may be incompatibilities between dependency versions.
```bash
pip install target-typo-proxy
```



### Create a configuration file

The config file (usually config.json) is a JSON file describing the tap's settings.

The following sample configuration can be used as a starting point:

```json
{
  "api_key": "my_apikey",
  "api_secret": "my_apisecret",
  "cluster_api_endpoint": "https://cluster.typo.ai/management/api/v1",
  "repository": "my_repository",
  "errors_target": "my_target -c errors.json",
  "valid_target": "my_target -c valid.json",
  "passthrough_target": "another_target -c passthrough.json"
}
```

- **api_key**, **api_secret** and **cluster_api_endpoint** can be obtained by logging into the [Typo Console](https://console.typo.ai/?utm_source=github&utm_medium=target-typo-proxy), clicking on your username, and then on **My Account**.
- **repository** corresponds to the target Typo Repository where the data will be stored. If not found, a Typo Dataset with the same name as the input stream name will be created in this Repository.
- **errors_target** - The command for the target that will receive records predicted as error.
- **valid_target** - The command for the target that will receive records predicted as valid. At least one of errors_target and valid_target must be provided (not necessarily both).
- **passthrough_target** - (optional) The command for the target that will receive all records.
- Additionally, some optional parameters can be provided:
  - **record_timeout**: integer value specifying the maximum evaluation time per record. Default: `1`. 
  - **fail_on_partial_results**: if false, and the per-record timeout is reached, Typo will still return partial results, otherwise it will fail the whole batch. Default: `true`.
  - **retry_times**: number of retries in case a per-record timeout is reached. Default: `4`.
  -  **send_threshold**: determines how many records will be sent to Typo in one batch. Default: `50`. Maximum value: `100`.
  - **disable_collection**: boolean property that prevents target-typo-proxy from sending anonymous usage data to Singer.io. Default: `false`.



### Run target-typo-proxy

```bash
> example-tap -c example_tap_config.json | target-typo-proxy -c config.json
```

Target-typo-proxy will execute the targets specified in the configuration file as subprocesses.



### Saving state

In order to save state, make sure you use a passthrough target and redirect the output of the target to a file.

Example (in your config.json file):

```bash
 {
  ...
  "passthrough_target": "some_target -c passthrough.json > state_history.txt"
  ...
}
```



## Typo registration and setup

In order to create a Typo account, visit [https://www.typo.ai/signup](https://www.typo.ai/signup?utm_source=github&utm_medium=target-typo-proxy) and follow the instructions.

Once registered you can log in to the Typo Console ([https://console.typo.ai/](https://console.typo.ai/?utm_source=github&utm_medium=target-typo-proxy)) and go to the Repositories section to create a new Repository.

Now you can start using target-typo-proxy. A new dataset will be created automatically when data is analyzed.



## Development

To work on development of target-typo-proxy, clone the repository, create and activate a new virtual environment, go into the cloned folder and install tap-typo in editable mode.

```bash
git clone https://github.com/typo-ai/target-typo-proxy.git
cd target-typo-proxy
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```



## Support

You may reach Typo Support at the email address support@ followed by the typo domain or see the full contact information at [https://www.typo.ai](https://www.typo.ai?utm_source=github&utm_medium=target-typo-proxy)



---

Copyright 2019-2020 Typo. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the License.

This product includes software developed at or by Typo ([https://www.typo.ai](https://www.typo.ai?utm_source=github&utm_medium=target-typo-proxy)).