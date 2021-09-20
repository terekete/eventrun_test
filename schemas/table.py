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
            'bds': {
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
    'dataset_id': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'table_id': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'expiration_ms': {
        'required': False,
        'type': 'string',
        'nullable': True
    },
    'friendly_name': {
        'required': True,
        'type': 'string',
        'nullable': False
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
    'access': {
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