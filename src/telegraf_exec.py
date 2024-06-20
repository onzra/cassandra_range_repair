import json

if __name__ == '__main__':
    json_file_path = '/var/tmp/repair_status.json'

    values = {}

    with open(json_file_path) as json_file:
        data = json.load(json_file)

    values['pending_repairs'] = len(data['pending_repairs'])
    values['current_repairs'] = len(data['current_repairs'])
    values['finished_repairs'] = len(data['finished_repairs'])
    values['failed_repairs'] = len(data['failed_repairs'])

    values_string = ','.join('{key}={value}'.format(key=k, value=values[k]) for k in values)
    print "cassandra_repair_progress %s" % (values_string)

