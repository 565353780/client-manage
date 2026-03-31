from client_manage.Module.cos_client_manager import COSClientManager


if __name__ == '__main__':
    data_url_path = 'bucket/key'
    data_file_path = 'local_path'

    cos_client_manager = COSClientManager()

    data_stream = cos_client_manager.getObjectStream(
        data_url_path=data_url_path,
        use_common_client=False,
    )

    cos_client_manager.downloadObject(
        data_url_path=data_url_path,
        data_file_path=data_file_path,
        use_common_client=False,
        overwrite=False,
        print_progress=True,
    )

    cos_client_manager.uploadObject(
        data_file_path=data_file_path,
        data_url_path=data_url_path,
        use_common_client=False,
        remove_after_upload=True,
        print_progress=True,
    )
