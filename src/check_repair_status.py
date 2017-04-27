#!/usr/bin/env python
"""
Script to check repair status on multiple nodes.

Uses SSH to connect to nodes and look for range_repair.py status output file. Aggregates status information for all
nodes.

Example:
    ./check_repair_status.py status.json cass-1.example.com cass-2.example.com cass-3.example.com

TODO:
- Add steps argument to output status JSON so we can do proper percentage calculation
- Add an option for simple human readable output
- Stop using print and do proper logging with debug option
- Make repair hang timeout an option
"""
import json
import os
import paramiko
from argparse import ArgumentParser
from datetime import datetime

STATUS_NO_DATA = 'no_data'
STATUS_FINISHED = 'finished'
STATUS_REPAIRING = 'repairing'
STATUS_FINISHED_WITH_ERRORS = 'finished_with_errors'
STATUS_HUNG = 'hung'

REPAIR_HANG_TIMEOUT_SECONDS = 3 * 60 * 60

ssh = paramiko.SSHClient()
ssh_config = paramiko.SSHConfig()


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

    for host in nodes:
        try:
            status_str = ssh_get_file(host, filename)
            status = json.loads(status_str)
            cluster[host] = build_node(status)
            cluster[host]['raw'] = status
        except Exception as e:
            print(str(e))
            cluster[host] = {
                'status': STATUS_NO_DATA
            }
        cluster['total_nodes'] += 1
        if cluster[host]['status'] == STATUS_NO_DATA:
            cluster['num_no_data'] += 1
        else:
            if cluster[host]['status'] == STATUS_FINISHED:
                cluster['num_fully_repaired'] += 1
            elif cluster[host]['status'] == STATUS_REPAIRING:
                cluster['num_repairing'] += 1
            elif cluster[host]['status'] == STATUS_FINISHED_WITH_ERRORS:
                cluster['num_repaired_with_errors'] += 1
            elif cluster[host]['status'] == STATUS_HUNG:
                cluster['num_hung'] += 1
            cluster['num_errors'] += cluster[host]['num_failed']
            percentage_total += cluster[host]['percentage_complete']
    cluster['percentage_complete'] = percentage_total / cluster['total_nodes']

    return cluster


def ssh_get_file(host, filename):
    """
    SSH into a host and get the contents of a file.

    SSHs into the host and executes a simple cat {filename}.

    :param str host: Host.
    :param str filename: Filename.

    :rtype: str
    :return: File contents.
    """
    global ssh
    cmd = 'cat {0}'.format(filename)
    print('ssh {0}: {1}'.format(host, cmd))
    cfg = get_ssh_config(host)
    ssh.connect(**cfg)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    out_str = "\n".join(map(str, ssh_stdout))
    err_str = "\n".join(map(str, ssh_stderr))
    ssh.close()
    if out_str:
        return out_str
    else:
        raise Exception('Failed to "{0}" on {1}: {2}'.format(cmd, host, err_str))


def get_ssh_config(host):
    """
    Get SSH config for host.

    :param str host: Hostname.

    :rtype: dict
    :return: SSHClient.connect params.
    """
    global ssh_config
    cfg = {'hostname': host}

    user_config = ssh_config.lookup(cfg['hostname'])
    for k in ('hostname', 'username', 'port'):
        if k in user_config:
            cfg[k] = user_config[k]

    if 'proxycommand' in user_config:
        cfg['sock'] = paramiko.ProxyCommand(user_config['proxycommand'])

    return cfg


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
    parser = ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('nodes', nargs='+')

    args = parser.parse_args()

    # Load SSH
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    user_config_file = os.path.expanduser('~/.ssh/config')
    if os.path.exists(user_config_file):
        with open(user_config_file) as f:
            ssh_config.parse(f)

    cluster = build_cluster(args.nodes, args.filename)

    print json.dumps(cluster, indent=4)
