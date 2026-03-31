import os


COS_CONFIG = {
    'secret_id': f'{os.environ["COS_SECRET_ID"]}',
    'secret_key': f'{os.environ["COS_SECRET_KEY"]}',
    'region': f'{os.environ["COS_REGION"]}',
    'coscli_path': f'{os.environ["COSCLI_PATH"]}',
}
