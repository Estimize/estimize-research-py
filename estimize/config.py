import os

DEFAULT_TIMEZONE = 'US/Eastern'
DEFAULT_START_DATE = '2012-01-01'
DEFAULT_END_DATE = '2018-03-31'

S3_DATA_BUCKET = 'com.estimize.production.data'
CURRENT_QUARTER = '2018q1'
ROOT_DATA_URL = 'https://s3.amazonaws.com/{}/research/{}'.format(S3_DATA_BUCKET, CURRENT_QUARTER)


def data_dir():
    if os.path.basename(os.getcwd()) == 'notebooks':
        return os.path.join(os.getcwd(), os.pardir, 'data')
    else:
        return os.path.join(os.getcwd(), 'data')
