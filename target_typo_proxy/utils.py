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

import collections


def flatten(data_json, parent_key='', sep='__'):
    '''
    Flattening JSON nested file
    Singer-provided default function
    '''
    items = []
    for json_object, json_value in data_json.items():
        new_key = parent_key + sep + json_object if parent_key else json_object
        if isinstance(json_value, collections.MutableMapping):
            items.extend(flatten(json_value, new_key, sep=sep).items())
        else:
            items.append((new_key, str(json_value) if isinstance(json_value, list) else json_value))
    return dict(items)
