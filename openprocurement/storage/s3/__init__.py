from boto.s3.connection import S3Connection, Location
from openprocurement.storage.s3.storage import S3Storage


def includeme(config):
    settings = config.registry.settings
    if 's3.access_key' in settings and 's3.secret_key' in settings and 's3.bucket' in settings:
        # S3 connection
        connection = S3Connection(settings['s3.access_key'], settings['s3.secret_key'])
        bucket_name = settings['s3.bucket']
        if bucket_name != 'test' and bucket_name not in [b.name for b in connection.get_all_buckets()]:
            connection.create_bucket(bucket_name, location=Location.EU)
        config.registry.storage = S3Storage(connection, bucket_name)
    else:
        raise
