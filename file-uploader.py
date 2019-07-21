import os
from io import BytesIO
# Import MinIO library.
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

# Initialize minioClient with an endpoint and access/secret keys.
minioClient = Minio('minio:9000',
                    access_key='AKIAIOSFODNN7EXAMPLE',
                    secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                    secure=False)

# Make a bucket with the make_bucket API call.           
#try:
#    minioClient.make_bucket("target", location="us-east-1")
#except ResponseError as err:
#    print(err)

# Put a file with 'application/csv'.
#try:
#    with open('my-testfile', 'rt') as file_data:
#        file_stat = os.stat('my-testfile')
#        minioClient.put_object('mybucket', 'my-testfile', file_data,
#                    file_stat.st_size, content_type='application/csv')
#
#except ResponseError as err:
#    print(err)
    
# Get a full object.
#try:
#    data = minioClient.get_object('mybucket', 'my-testfile')
#    #with open('my-testfile', 'wb') as file_data:
#    for d in data.stream(32*1024):
#        print(d)
#        #file_data.write(d)
#except ResponseError as err:
#    print(err)
    
# move file
try:
    print("trying to move file ...")
    data = minioClient.get_object('mybucket', 'my-testfile')
    content = data.read()
    minioClient.put_object('target', 'my-testfile', BytesIO(content),
                    len(content), content_type='application/csv')
except ResponseError as err:
    print(err)
