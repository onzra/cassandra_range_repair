#!/usr/bin/env python
"""
Script to check repair status on multiple nodes.

Uses SSH to connect to nodes and look for range_repair.py status output file. Aggregates status information for all
nodes.

{
    "current_repair": {
        "cmd": "nodetool -h localhost -p 7199 repair cisco_test -pr    -st +09176475922996267832 -et +09188223637297142667",
        "column_families": "<all>",
        "end": "+09188223637297142667",
        "keyspace": "cisco_test",
        "nodeposition": "256/256",
        "start": "+09176475922996267832",
        "step": 1,
        "time": "2017-04-26T03:44:42.543667"
    },
    "failed_count": 1,
    "failed_repairs": [
        {
            "cmd": "nodetool -h localhost -p 7199 repair cisco_test -pr    -st -08956690834811572306 -et -08935863217669227885",
            "column_families": "<all>",
            "end": "-08935863217669227885",
            "keyspace": "cisco_test",
            "nodeposition": "5/256",
            "start": "-08956690834811572306",
            "step": 1,
            "time": "2017-04-26T03:44:41.562615"
        }
    ],
    "finished": "2017-04-26T03:44:42.544609",
    "started": "2017-04-26T03:44:41.546225",
    "successful_count": 256,
    "updated": "2017-04-26T03:44:42.544623"
}
"""
import paramiko
import json
from datetime import datetime

STATUS_NO_DATA = 'no_data'
STATUS_FINISHED = 'finished'
STATUS_REPAIRING = 'repairing'
STATUS_FINISHED_WITH_ERRORS = 'finished_with_errors'
STATUS_HUNG = 'hung'

REPAIR_HANG_TIMEOUT_SECONDS = 3 * 60 * 60


def ssh_get_file(host, filename):
    cmd = 'cat {0}'.format(filename)
    print('ssh {0}: {1}'.format(host, cmd))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    out_str = "\n".join(map(str, ssh_stdout))
    err_str = "\n".join(map(str, ssh_stderr))
    ssh.close()
    if out_str:
        return out_str
    else:
        raise Exception('Failed to "{0}" on {1}: {2}'.format(cmd, host, err_str))


def build_cluster(nodes, filename):
    """
    Build cluster status object.

    :param list nodes: List of nodes to check.
    :param str filename: Status filename.

    :rtype: dict
    :return: Cluster status object.
    """
    cluster = {
        'total_nodes': 0,
        'num_no_data': 0,
        'num_fully_repaired': 0,
        'num_repairing': 0,
        'num_repaired_with_errors': 0,
        'num_hung': 0,
        'num_errors': 0,
    }
    percentage_total = 0

    for node in nodes:
        try:
            status_str = ssh_get_file(node, filename)
            status = json.loads(status_str)
            cluster[node] = build_node(status)
            # cluster[node]['raw'] = status
        except Exception as e:
            print(str(e))
            cluster[node] = {
                'status': STATUS_NO_DATA
            }
        cluster['total_nodes'] += 1
        if cluster[node]['status'] == STATUS_NO_DATA:
            cluster['num_no_data'] += 1
        else:
            if cluster[node]['status'] == STATUS_FINISHED:
                cluster['num_fully_repaired'] += 1
            elif cluster[node]['status'] == STATUS_REPAIRING:
                cluster['num_repairing'] += 1
            elif cluster[node]['status'] == STATUS_FINISHED_WITH_ERRORS:
                cluster['num_repaired_with_errors'] += 1
            elif cluster[node]['status'] == STATUS_HUNG:
                cluster['num_hung'] += 1
            cluster['num_errors'] += cluster[node]['num_failed']
            percentage_total += cluster[node]['percentage_complete']
    cluster['percentage_complete'] = percentage_total / cluster['total_nodes']

    return cluster


def build_node(node_status):
    """
    Build node status object.

    Gather some summary metrics from raw node status.

    :param dict node_status: Node's repair status object.

    :rtype: dict
    :return: Node status object.
    """
    num_failed = len(node_status['failed_repairs'])
    started = datetime.strptime(node_status['started'], '%Y-%m-%dT%H:%M:%S.%f')
    updated = datetime.strptime(node_status['updated'], '%Y-%m-%dT%H:%M:%S.%f')
    if node_status['finished']:
        # Hacky
        node_position = '256/256'
        finished_on = node_status['finished']
        current_step_time = None
        finished = datetime.strptime(node_status['finished'], '%Y-%m-%dT%H:%M:%S.%f')
        total_repair_time = (finished - started).total_seconds()
        if num_failed > 0:
            status = STATUS_FINISHED_WITH_ERRORS
        else:
            status = STATUS_FINISHED
    else:
        node_position = node_status['current_repair']['nodeposition']
        current_step_time = (datetime.now() - updated).total_seconds()
        finished_on = None
        total_repair_time = None
        if current_step_time > REPAIR_HANG_TIMEOUT_SECONDS:
            status = STATUS_HUNG
        else:
            status = STATUS_REPAIRING
    (current_vnode, total_vnodes) = map(int, node_position.split('/'))
    # Note this calculation assumes steps of 1 (need to put this param in status output to do proper calc)
    percentage_complete = int(float(current_vnode - num_failed) / float(total_vnodes) * 100)
    return {
        'status': status,
        'node_position': node_position,
        'percentage_complete': percentage_complete,
        'current_step_time_seconds': current_step_time,
        'total_repair_time_seconds': total_repair_time,
        'num_failed': num_failed,
        'finished': finished_on
    }


if __name__ == '__main__':
    nodes = ['cass-226-1.onzra.com', 'cass-226-2.onzra.com', 'cass-226-3.onzra.com']
    filename = 'status.json'

    cluster = build_cluster(nodes, filename)

    print json.dumps(cluster, indent=4)
