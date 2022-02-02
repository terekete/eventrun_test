{
    'version': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'kind': {
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
                'required': True,
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
    'description': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'resource_name': {
        'required': True,
        'type': 'string',
        'nullable': False,
        'regex': '^[a-z0-9_-]+$'
    },
    'partition_expiration_ms': {
        'required': False,
        'type': 'number',
        'nullable': True
    },
    'table_expiration_ms': {
        'required': False,
        'type': 'number',
        'nullable': True
    },
    'users': {
        'required': True,
        'type': 'dict',
        'schema': {
            'readers': {
                'required': True,
                'type': 'list',
                'nullable': True,
                'regex': '^(user:|serviceAccount:)([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+)\.([a-zA-Z0-9-.]+)$'
            },
            'writers': {
                'required': True,
                'type': 'list',
                'nullable': True,
                'regex': '^(user:|serviceAccount:)([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+)\.([a-zA-Z0-9-.]+)$'
            }
        }
    }
}