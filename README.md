# target-typo-proxy

[Singer](https://singer.io) target that provides data quality checking with [Typo](https://www.typo.ai/) for data in transit.

*target-typo-proxy* works as a standard Singer target that provides AI assisted data quality by sending the received data to Typo for inspection. Depending
on the results returned by Typo, *target-typo-proxy* forwards the record to a valid or error target. An optional passthrough target receives all records regardless of Typo results.

## Install

Requires Python 3
```bash
pip install target-typo-proxy
```

## Use
target-typo-proxy takes two types of input:
1. A stream of Singer-formatted data on stdin
2. A config file containing:
    1. api_key - API Key created in the Typo Dashboard
    2. api_secret - API secret created in the Typo Dashboard
    3. cluster_api_endpoint - URL to your Typo cluster.  See your account details on the Typo Dashboard.
    4. repository - Name of the repository
    5. send_threshold - The batch size in number of rows to send in each import request to Typo
    6. errors_target - The command for the target that will receive records predicted as error
    7. valid_target - The command for the target that will receive records predicted as valid
    8. passthrough_target - (optional) The command for the target that will receive all records

At least one of errors_target and valid_target must be provided.

Sample config file:
```json
{
  "disable_collection": false,
  "api_key": "my_apikey",
  "api_secret": "my_apisecret",
  "cluster_api_endpoint": "https://cluster.typo.ai/management/api/v1",
  "repository": "my_repository",
  "send_threshold": 250,
  "errors_target": "my_target -c errors.json",
  "valid_target": "my_target -c valid.json",
  "passthrough_target": "another_target -c passthrough.json"
}
```

```bash
> TAP-some-api | target-typo-proxy -c config.json
```

## Development Install
```
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Support

You may reach Typo Support at the email handle support@ then the typo domain or See the contact information at [https://www.typo.ai](https://www.typo.ai/)

---

Copyright 2019 Typo. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the
License.

You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

This product includes software developed at
or by Typo (https://www.typo.ai/).
