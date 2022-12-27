import re, os
import configparser

def get_aws_keys_input():
    '''
    ask user for  aws_access_key,aws_secret_key in aws_cfg.ini, ask user for input if not already present
    '''
    cfg_fname=r"aws_cfg.ini"
    aws_access_key=input('please input aws access key:')
    while not is_valid_access_key(aws_access_key):
        print('not a valid aws access key')
        aws_access_key=input('please input aws access key (20 characters):')

    aws_secret_key=input('please input aws secret key:')
    while not is_valid_secret_key(aws_secret_key):
        print('not a valid aws secret key, try again')
        aws_secret_key=input('please input aws secret key (40 characters):')

    bucket_name=input('please input s3 bucket name')
    
    config = configparser.ConfigParser()
    # Add the structure to the file we will create
    config.add_section('aws')
    config.set('aws', 'access_key', aws_access_key)
    config.set('aws', 'secret_key', aws_secret_key)
    config.set('aws', 'bucket_name', bucket_name)

    # Write the new structure to the new file
    with open(cfg_fname, 'w') as configfile:
        config.write(configfile)

def read_aws_config(cfg_fname="aws_cfg.ini", secion='aws'):
    import os
    import configparser
    if not os.path.exists(cfg_fname):
        get_aws_keys_input()
    config = configparser.ConfigParser()
    try:
        import configparser

        # Add the structure to the file we will create
        config.read(cfg_fname)

        return config[secion]

    except:
        get_aws_keys_input()
        config.read(cfg_fname)
        return config[secion]


        
def is_valid_access_key(key):
    import re
    pattern = r'^[A-Z0-9]{20}$'
    return bool(re.match(pattern, key))


def is_valid_secret_key(key):
    pattern = r'^[A-Za-z0-9/+]{40}$'
    return bool(re.match(pattern, key))

class cfgUtil:

    @staticmethod
    def get_aws_keys_from_ini(cfg_fname="aws_cfg.ini"):
        '''
        return dict with key access_key, secret_key
        '''
        config=read_aws_config(cfg_fname)      
        return dict(config)
    

