import os
import time
import boto3
import configparser
import datetime

# Parse the sync.ini to get the aws credential
import pandas as pd

# setup ignore list for files to ignore
def setup_ignore_list():
    ignore_list = []
    ignore_list.append('.DS_Store')
    ignore_list.append('.update_info')
    return ignore_list


def run_sync_daemon(sync_dir, counter=0):
    # Connect to the S3 service
    from datalib.cfgUtil import cfgUtil
    aws_key_dict=cfgUtil.get_aws_keys_from_ini()
    AWS_ACCESS_KEY = aws_key_dict['access_key']
    AWS_SECRET_KEY = aws_key_dict['secret_key']
    bucketname=aws_key_dict['bucket_name']
    ignore_list=setup_ignore_list()
    print('bucketname is ',bucketname)
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    force_check=False

    current_time = time.time() 
    # Read the last update time from .update_info
    tsfile='.update_info'

    last_check_time=0
    if os.path.exists(tsfile):
        try:
            with open(tsfile, 'r') as f:
                last_check_time = int(float(f.read()))
        except:
            print('no previous update time')

    if force_check:
        last_check_time=0


    all_files=[]
    # Iterate through all the files in the local S3dropbox folder
    for root, dirs, files in os.walk(sync_dir):
        for file in files:
            print('checking file ',file)
            file_path = os.path.join(root, file)
            all_files.append(file_path)
            # Check if the file is newer than the timestamp
                # Check if the file was modified after the last update time
            if file in ignore_list:
                continue
            modified_time=os.path.getmtime(file_path) 
            modified_timestamp = pd.to_datetime(modified_time, unit='s').timestamp()
            if modified_timestamp > last_check_time:
                # Upload the file to the S3 bucket
                print('upload file ',file)            
                s3.upload_file(file_path, bucketname, f'{sync_dir}/' + file)
            else:
                print('no change for file ',file)



    # Update the last update time in .update_info
    with open('.update_info', 'w') as f:
        f.write(str(current_time))


    # Now we need to check for any files that are in the S3 bucket but not in the local S3dropbox folder
    objects = s3.list_objects_v2(Bucket=bucketname, Prefix=f'{sync_dir}/')
    #objects = s3.list_objects_v2(Bucket=bucketname)

    # get current time with tz
    from dateutil.tz import tzutc
    import datetime
    now=datetime.datetime.now(tz=tzutc())


    # Iterate through the objects in the S3 bucket
    import datetime
    print('obj key:',objects.keys())
    if 'Contents' in objects.keys():
        for obj in objects['Contents']:
            print('obj is ',obj)
            print('time diff =',now-obj['LastModified'])
            # Check if the object is not in the local S3dropbox folder
            fullfname=obj['Key']
            print('full fname:',fullfname)
            #check if still exist locally
            if not os.path.exists(fullfname):
                # Move the object to the deleted folder
                #remove file already deleted in local
                print('xx')
                s3.copy_object(Bucket=bucketname, CopySource={'Bucket': bucketname, 'Key': obj['Key']}, Key=f'{sync_dir}_deleted/' + obj['Key'][10:])
                s3.delete_object(Bucket=bucketname, Key=obj['Key'])
    
    if cnt%1000==0:
        # do some house keep every 1000 cycles:
        list_all_files_and_get_total_size(s3, bucketname, delete=False)
                
def list_all_files_and_get_total_size(s3, bucketname='jls3b1', delete=False):
    all_flist=[]
    objects2 = s3.list_objects_v2(Bucket=bucketname)
    tsize=0
    if not 'Contents' in objects2.keys():
        print('s3 is empty')
        return 0
    for obj in objects2['Contents']:
        key = obj['Key']
        size = obj['Size']
        dirname=os.path.dirname(key)
        print(f'{key}: {size} bytes {dirname}')
        tsize=size+tsize
        if '_deleted'==dirname[-8:]:
            td=(datetime.datetime.now(tz=tzutc())-obj['LastModified'])
            if td.seconds>3600*24*10:
                print('binned for more than 10 days, now removing %s' % obj['Key'])
                s3.delete_object(Bucket=bucketname, Key=key)                
            continue
        if delete:
            s3.delete_object(Bucket=bucketname, Key=key)
            continue
        all_flist.append(key)
            

    bucket_size=tsize    
    ugb=bucket_size / 1024 / 1024 / 1024
    print(f'Bucket {bucketname} is using {ugb:.2f} GB of storage')     
    ret_dict={}
    ret_dict['flist']=all_flist
    ret_dict['tsize']=tsize
    return ret_dict


if __name__=='__main__':                
    cnt=0
    sync_dir='S3dropbox'
    while(True):
        run_sync_daemon(sync_dir, counter=cnt)
        time.sleep(60)
        cnt=cnt+1