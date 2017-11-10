DEFAULT_TIMEZONE = 'US/Eastern'
DEFAULT_START_DATE = '2012-01-01'
DEFAULT_END_DATE = '2017-10-01'

S3_DATA_BUCKET = 'com.estimize.production.data'
CURRENT_QUARTER = '2017q3'
ROOT_DATA_URL = 'https://s3.amazonaws.com/{}/research/{}'.format(S3_DATA_BUCKET, CURRENT_QUARTER)
