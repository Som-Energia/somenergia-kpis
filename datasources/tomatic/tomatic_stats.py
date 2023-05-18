import pandas as pd
import sys
import paramiko
from pathlib import Path
import yaml
from operator import attrgetter

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def setup_ssh_connection(username, password, hostname, sshport):

    logging.info(f'Opening SSH connection')
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username, password=password, port=sshport)
    return client.open_sftp()

def get_csv_by_ssh(sftp, dbapi, schema, remotepath):

    logging.info('Reading stats.csv')
    with sftp.open(remotepath+'/stats.csv') as f:
        calls = pd.read_csv(f, sep='\t')
        calls['DATE'] = pd.to_datetime(calls['DATE'], utc=True).dt.date
        calls.columns = calls.columns.str.lower()
        calls.to_sql("tomatic_stats", con=dbapi, schema=schema, if_exists='replace', index=False)


def get_yamls_by_ssh(sftp, dbapi, schema, remotepath):

    stats_dir = Path('stats')

    remotefullpath = remotepath / stats_dir

    # Let's load everything in RAM since we'll replace all this by GOING TO THE SOURCE
    allcalls = pd.DataFrame()

    call_files = sorted(sftp.listdir_attr(str(remotefullpath)), key=attrgetter('filename'))
    logging.info(f"Download {len(call_files)} call files")

    for f in call_files:
        if Path(f.filename).match('*.yaml'):
            logging.info(f'Processing file {f.filename}')
            with sftp.open(str(remotefullpath/f.filename)) as file:
                logging.info(f'yaml.load {f.filename}')
                data = yaml.load(file, Loader=yaml.FullLoader)
                logging.info(f'pd.DataFrame.from_dict {f.filename}')
                calls = pd.DataFrame.from_dict(data)
                logging.info(f'pd.append {f.filename}')
                allcalls = allcalls.append(calls, ignore_index=True)

    calls.to_sql("tomatic_calls_hourly", con=dbapi, schema=schema, if_exists='replace', index=False)


if __name__ == '__main__':

    dbapi = sys.argv[1]
    schema = sys.argv[2]
    username = sys.argv[3]
    password = sys.argv[4]
    hostname = sys.argv[5]
    sshport = sys.argv[6]
    remotepath = sys.argv[7]
    # remotepath = '/opt/www/somenergia-tomatic/stats.csv'

    logging.info(f'Starting script')
    sftp = setup_ssh_connection(username, password, hostname, sshport)
    get_yamls_by_ssh(sftp, dbapi, schema, remotepath)
    get_csv_by_ssh(sftp, dbapi, schema, remotepath)

    logging.info("Job's Done, Have A Nice Day")
