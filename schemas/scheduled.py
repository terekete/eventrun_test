{
    'version': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'type': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'metadata': {
        'required': True,
        'type': 'dict',
        'schema': {
            'cost_center': {
                'required': True,
                'type': 'string',
                'nullable': False
            },
            'dep': {
                'required': True,
                'type': 'string',
                'nullable': False
            },
            'bds_email': {
                'required':  True,
                'type': 'string',
                'nullable': False
            }
        }
    },
    'team': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'resource_name': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'data_refresh_window_days': {
        'required': False,
        'type': 'number',
        'nullable': True
    },
    'destination_dataset_id': {
        'required': True,
        'type': 'string',
        'nullable': True
    },
    'destination_table_id': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'write_disposition': {
        'required': True,
        'type': 'string',
        'nullable': False                
    },
    'query': {
        'required': True,
        'type': 'string',
        'nullable': False            
    },
    'dependencies': {
        'required': False,
        'type': 'list',
        'nullable': False
    }
}