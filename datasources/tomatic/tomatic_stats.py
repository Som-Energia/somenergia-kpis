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

    colnames = ['calldate', 'id_stat', 'ivoz.asteriskcallid', 'ivoz.sipcallid',
        'ivoz.uniqueid', 'origsipcallid', 'karmausuario', 'extensiondestino',
        'clidnum', 'clidname', 'channel', 'dstchannel', 'peerin', 'peerout',
        'dcontext', 'lastapp', 'lastdata', 'nombrecentrocostedestino',
        'nombredestino', 'uniqueid', 'modulo', 'src', 'srcagent', 'dst',
        'billsec', 'agent_time', 'wait_time', 'ring_time', 'platform', 'cola',
        'queuename', 'call_type', 'call_subtype', 'hangupcause',
        'cdr_disposition', 'orig_pos', 'abandon_pos', 'xfer_dst_billsec',
        'xfer_src_billsec', 'ano', 'mes', 'dia', 'hora', 'minuto', 'fecha',
        'virtual_did', 'call_descr', 'did_in', 'queue_agent_priority',
        'custom_field_2', 'custom_field_3', 'id_dialplan_partition',
        'dialplan_partition', 'pata1.localizada', 'pata2.localizada', 'utctime',
        'localtime', 'extensionorigen', 'ruta', 'nombrecentrocosteorigen',
        'nombreorigen', 'origin_filename']

    # Let's load everything in RAM since we'll replace all this by GOING TO THE SOURCE

    call_files = sorted(sftp.listdir_attr(str(remotefullpath)), key=attrgetter('filename'))
    logging.info(f"Download {len(call_files)} call files")

    query = f'select distinct origin_filename from {schema}.tomatic_calls_hourly'
    calls_files_done = pd.read_sql_query(query, con=dbapi)

    file_to_do = [f for f in call_files if f.filename not in calls_files_done['origin_filename'].values.tolist()]

    logging.info(f"{len(file_to_do)} new files")

    for f in file_to_do:
        if Path(f.filename).match('*.yaml'):
            logging.info(f'Processing file {f.filename}')
            with sftp.open(str(remotefullpath/f.filename)) as file:
                data = yaml.load(file, Loader=yaml.FullLoader)
            calls = pd.DataFrame.from_dict(data['calls'])
            calls['origin_filename'] = f.filename
            calls = calls.rename(columns={'@calldate':'calldate'})
            calls = calls[calls.columns.intersection(colnames)]
            calls['localtime'] = calls['localtime'].dt.tz_localize(None)
            calls.to_sql("tomatic_calls_hourly", con=dbapi, schema=schema, if_exists='append', index=False)

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
