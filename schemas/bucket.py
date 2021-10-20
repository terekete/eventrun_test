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
    'resource_name': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'storage_class': {
        'required': False,
        'type': 'string',
        'nullable': True,
        'allowed': ['STANDARD', 'ARCHIVE', 'NEARLINE']
    },
    'lifecycle_type': {
        'required': False,
        'type': 'string',
        'nullable': True,
        'allowed': ['SetStorageClass', 'Delete']
    },
    'lifecycle_age_days': {
        'required': False,
        'type': 'string',
        'nullable': True
    },
    'dependencies': {
        'required': False,
        'type': 'list',
        'nullable': True
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