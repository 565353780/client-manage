pip install -U cos-python-sdk-v5

cd ..
mkdir MFS
cd MFS
bash /home/tangpei/mfs/bin/build_client.sh

TARGET_DIR=$(python -c "import site; print(site.getsitepackages()[0])")
cp ./MFSClient.*.so $TARGET_DIR/
