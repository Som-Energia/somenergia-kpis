import pandas as pd
import sys
import paramiko


def get_csv_by_ssh(dbapi, schema, username, password, hostname, sshport, remotepath):

    # open an SSH connection
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username, password=password, port=sshport)

    # read the file using SFTP
    sftp = client.open_sftp()
    with sftp.open(remotepath) as f:
        calls = pd.read_csv(f, sep='\t')
        calls['DATE'] = pd.to_datetime(calls['DATE'], utc=True).dt.date
        calls.to_sql("tomatic_stats", con=dbapi, schema=schema, if_exists='replace', index=False)


if __name__ == '__main__':

    dbapi = sys.argv[1]
    schema = sys.argv[2]
    username = sys.argv[3]
    password = sys.argv[4]
    hostname = sys.argv[5]
    sshport = sys.argv[6]
    remotepath = sys.argv[7]
    # remotepath = '/opt/www/somenergia-tomatic/scripts/stats.csv'

    get_csv_by_ssh(dbapi, schema, username, password, hostname, sshport, remotepath)
