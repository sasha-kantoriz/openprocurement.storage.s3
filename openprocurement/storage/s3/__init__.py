from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from openprocurement.storage.s3.storage import S3Storage


def includeme(config):
    settings = config.registry.settings
    if ('s3.access_key' in settings and 's3.secret_key' in settings and
            's3.bucket' in settings):
        # S3 connection
        if 's3.host' in settings and 's3.port' in settings:
            connection = S3Connection(
                aws_access_key_id=settings['s3.access_key'],
                aws_secret_access_key=settings['s3.secret_key'],
                is_secure=False,
                host=settings['s3.host'],
                port=int(settings['s3.port']),
                calling_format=OrdinaryCallingFormat())
        else:
            connection = S3Connection(settings['s3.access_key'],
                                      settings['s3.secret_key'])
        bucket_name = settings['s3.bucket']
        config.registry.storage = S3Storage(connection, bucket_name)
    else:
        raise
