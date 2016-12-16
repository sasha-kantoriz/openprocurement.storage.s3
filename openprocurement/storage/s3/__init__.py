from boto.s3.connection import S3Connection
from openprocurement.storage.s3.storage import S3Storage


def includeme(config):
    settings = config.registry.settings
    s3_settings = {}
    if 's3.access_key' in settings and 's3.secret_key' in settings and 's3.bucket' in settings:
        # S3 connection
        for k, v in settings.items():
            if k[:3] != 's3.':
                continue
            key = k[3:]
            if key == 'secret_key':
                s3_settings['aws_secret_access_key'] = v
            elif key == 'access_key':
                s3_settings['aws_access_key_id'] = v
            elif key == 'bucket':
                pass
            else:
                s3_settings[key] = v
        connection = S3Connection(**s3_settings)
        bucket_name = settings['s3.bucket']
        config.registry.storage = S3Storage(connection, bucket_name)
    else:
        raise
