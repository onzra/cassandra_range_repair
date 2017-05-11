import argparse
import requests
import json

base_url = None
database = None

# Replace any column family or keyspace matching '<all>' with 'ALL'.
ALL_REPLACEMENT = ('<all>', 'ALL')


def create_database():
    """
    InfluxDB create database is idempotent - if it exists, nothing happens. If it does not, database  will be created. 
    Influx follows a "no news is good news" error reporting philosophy.
    
    :return requests.response: response.
    """
    url = '{base_url}/query'.format(base_url=base_url)
    response = requests.post(url=url, data={'q': 'CREATE DATABASE {database}'.format(database=database)})
    return response


def insert_data(tags, values):
    """
    Insert into InfluxDB using the Influx REST API. The data is sent in the following format:
    curl -X POST '<server_url>/write?db=<database>' --data-binary '<measurement>,<tags> <values>'
        
    :param str tags: string of comma separated tags in format <tag_name>=<tag_value>,[...].
    :param str values: string of comma separated values in format <value_name>=<value>,[...].
    :return requests.response: response.
    """
    url = '{base_url}/write?db={database}'.format(base_url=base_url, database=database)
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    if tags.strip() != '':
        tags = ',{0}'.format(tags)

    data = 'current_repair{tags} {values}'.format(tags=tags, values=values)
    response = requests.post(url=url, data=data, headers=headers)
    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', required=True, help='The InfluxDB server for which to post data.')
    parser.add_argument('--port', default='8086', help='The InfluxDB server port.')
    parser.add_argument('--database', default='nodetool_repair', help='Database to use.')
    parser.add_argument('--json-file', default='/var/tmp/repair_status.json', help='Path to JSON file with data.')
    parser.add_argument('--hostname', help='Hostname tag.')

    args = parser.parse_args()

    server = args.server
    port = args.port
    database = args.database
    hostname = args.hostname
    json_file_path = args.json_file

    base_url = 'http://{server}:{port}'.format(server=server, port=port)

    create_database()

    with open(json_file_path) as json_file:
        data = json.load(json_file)

    tags = {}
    values = {}

    if hostname:
        tags['hostname'] = hostname

    if 'current_repair' in data and data['current_repair']:
        current_vnode = data['current_repair']['nodeposition'].split('/')[0]
        values['current_vnode'] = current_vnode

        # If current repair is available we can add tag data which will allow filtering and aggregation options.
        tags['keyspace'] = data['current_repair']['keyspace'].replace(*ALL_REPLACEMENT)
        # TODO: In the future this may change to a list object for multiple column family repairs.
        tags['column_family'] = data['current_repair']['column_families'].replace(*ALL_REPLACEMENT)
    else:
        values['current_vnode'] = 0

    if data['finished']:
        values['current_vnode'] = 0

    values['failed_count'] = data['failed_count']

    values_string = ','.join('{key}={value}'.format(key=k, value=values[k]) for k in values)
    tags_string = ','.join('{key}={value}'.format(key=k, value=tags[k]) for k in tags)

    insert_data(tags_string, values_string)
