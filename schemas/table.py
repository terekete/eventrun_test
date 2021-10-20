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
        'nullable': False
    },
    'dataset_name': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'expiration_ms': {
        'required': False,
        'type': 'string',
        'nullable': True
    },
    'dependencies': {
        'required': False,
        'type': 'list',
        'nullable': True
    },
    'schema': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'users': {
        'required': True,
        'type': 'dict',
        'schema': {
            'readers': {
                'required': True,
                'type': 'list',
                'nullable': True
            },
            'writers': {
                'required': True,
                'type': 'list',
                'nullable': True
            }
        }
    }
}