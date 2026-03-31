import os

from client_manage.Method.path import removeFile, createFileFolder

from client_manage.Client.cos import get_cos_client


def loadModelFn(
    model_file_path: str,
) -> bool:
    if os.path.exists(model_file_path):
        return True

    createFileFolder(model_file_path)

    bucket = 'mm-users-data-1303205185'
    key = 'lichanghao/' + model_file_path.split('/lichanghao/')[1]

    print('[INFO][cos_io::loadModelFn]')
    print('\t start download model:')
    print('\t', bucket + '/' + key)
    print('\t -->')
    print('\t', model_file_path)

    get_cos_client().download_file(
        Bucket=bucket, # Bucket名称
        Key=key,      # 上传到COS后的对象键
        DestFilePath=model_file_path, # 本地文件路径
        PartSize=20,                    # 分块大小(MB)，默认可不设
        MAXThread=10,                   # 并发线程数
    )
    print('[INFO][cos_io::loadModelFn]')
    print('\t download_file finished!')
    return True

def saveModelFn(
    model_file_path: str,
) -> bool:
    if not os.path.exists(model_file_path):
        print('[ERROR][cos_io::save_model_fn]')
        print('\t model file not exist!')
        return False

    bucket = 'mm-users-data-1303205185'
    key = 'lichanghao/' + model_file_path.split('/lichanghao/')[1]

    print('[INFO][cos_io::saveModelFn]')
    print('\t start upload model:')
    print('\t', model_file_path)
    print('\t -->')
    print('\t', bucket + '/' + key)

    response = get_cos_client().upload_file(
        Bucket=bucket, # Bucket名称
        Key=key,      # 上传到COS后的对象键
        LocalFilePath=model_file_path, # 本地文件路径
        PartSize=20,                    # 分块大小(MB)，默认可不设
        MAXThread=10,                   # 并发线程数
        EnableMD5=False,                # 开启MD5校验，大文件建议关闭以提速
    )
    print('[INFO][cos_io::saveModelFn]')
    print('\t', response['ETag']) # 成功后返回文件的ETag

    removeFile(model_file_path)
    return True
