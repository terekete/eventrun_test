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
    'bucket_name': {
        'required': True,
        'type': 'string',
        'nullable': False
    },
    'retention_seconds': {
        'required': False,
        'type': 'number',
        'nullable': True
    },
    'storage_class': {
        'required': False,
        'type': 'string',
        'nullable': True
    },
    'lifecycle_type': {
        'required': False,
        'type': 'string',
        'nullable': True
    },
    'lifecycle_storage_class': {
        'required': False,
        'type': 'number',
        'nullable': True
    },
    'lifecycle_age_days': {
        'required': False,
        'type': 'string',
        'nullable': True
    }
}