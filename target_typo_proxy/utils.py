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
