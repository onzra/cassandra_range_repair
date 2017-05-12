#!/usr/bin/env python
"""
Script to check repair status on multiple nodes.

Uses SSH to connect to nodes and look for range_repair.py status output file. Aggregates status information for all
nodes.

Example:
    ./check_repair_status.py status.json cass-1.example.com cass-2.example.com cass-3.example.com
"""
import json
import os
import paramiko
import logging
import sys
import csv
from argparse import ArgumentParser
from datetime import datetime

STATUS_NO_DATA = 'no_data'
STATUS_FINISHED = 'finished'
STATUS_REPAIRING = 'repairing'
STATUS_FINISHED_WITH_ERRORS = 'finished_with_errors'
STATUS_HUNG = 'hung'

NUM_VNODES = 256

DEFAULT_HANG_TIMEOUT = 10800

ssh = paramiko.SSHClient()
ssh_config = paramiko.SSHConfig()


def build_cluster(nodes, filename, hang_timeout=DEFAULT_HANG_TIMEOUT):
    """
    Build cluster status object.

    :param list nodes: List of nodes to check.
    :param str filename: Status filename.
    :param int hang_timeout: Repair hang timeout in seconds.

    :rtype: dict
    :return: Cluster status object.
    """
    cluster = {
        'nodes': {},
        'total_nodes': 0,
        'num_no_data': 0,
        'num_fully_repaired': 0,
        'num_repairing': 0,
        'num_repaired_with_errors': 0,
        'num_hung': 0,
        'num_errors': 0,
    }
    percentage_total = 0
    vnode_time = [0, 0]

    for host in nodes:
        try:
            status_str = ssh_get_file(host, filename)
            status = json.loads(status_str)
            cluster['nodes'][host] = build_node(status, hang_timeout)
            cluster['nodes'][host]['raw'] = status
        except Exception as e:
            logging.error(str(e))
            cluster['nodes'][host] = {
                'status': STATUS_NO_DATA
            }
        cluster['total_nodes'] += 1
        if cluster['nodes'][host]['status'] == STATUS_NO_DATA:
            cluster['num_no_data'] += 1
        else:
            if cluster['nodes'][host]['status'] == STATUS_FINISHED:
                cluster['num_fully_repaired'] += 1
            elif cluster['nodes'][host]['status'] == STATUS_REPAIRING:
                cluster['num_repairing'] += 1
            elif cluster['nodes'][host]['status'] == STATUS_FINISHED_WITH_ERRORS:
                cluster['num_repaired_with_errors'] += 1
            elif cluster['nodes'][host]['status'] == STATUS_HUNG:
                cluster['num_hung'] += 1
            cluster['num_errors'] += cluster['nodes'][host]['num_failed']
            percentage_total += cluster['nodes'][host]['percentage_complete']
            vnode_time[0] += cluster['nodes'][host]['avg_vnode_time_seconds']
            vnode_time[1] += float(1)
    cluster['percentage_complete'] = percentage_total / cluster['total_nodes']
    cluster['avg_vnode_time_seconds'] = vnode_time[0] / vnode_time[1]
    # Calculate estimated cluster repair time after so we can use avg time for nodes with no data
    cluster['est_full_repair_time_seconds'] = 0
    for host in nodes:
        if cluster['nodes'][host]['status'] == STATUS_FINISHED:
            cluster['est_full_repair_time_seconds'] += cluster['nodes'][host]['total_repair_time_seconds']
        elif cluster['nodes'][host]['status'] == STATUS_NO_DATA:
            cluster['est_full_repair_time_seconds'] += cluster['avg_vnode_time_seconds'] * NUM_VNODES
        else:
            cluster['est_full_repair_time_seconds'] += cluster['est_full_repair_time_seconds']

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
    logging.info('Checking repair status on {0}'.format(host))
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
    cfg = {
        'hostname': host
    }

    user_config = ssh_config.lookup(cfg['hostname'])
    for k in ('hostname', 'username', 'port'):
        if k in user_config:
            cfg[k] = user_config[k]

    if 'proxycommand' in user_config:
        cfg['sock'] = paramiko.ProxyCommand(user_config['proxycommand'])

    return cfg


def build_node(node_status, hang_timeout=DEFAULT_HANG_TIMEOUT):
    """
    Build node status object.

    Gather some summary metrics from raw node status.

    :param dict node_status: Node's repair status object.
    :param int hang_timeout: Hang timeout in seconds.

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
        current_step_time = (datetime.utcnow() - updated).total_seconds()
        finished_on = None
        total_repair_time = None
        if current_step_time > hang_timeout:
            status = STATUS_HUNG
        else:
            status = STATUS_REPAIRING
    (current_vnode, total_vnodes) = map(int, node_position.split('/'))
    # Note this calculation assumes steps of 1 (need to put this param in status output to do proper calc)
    percentage_complete = int(float(current_vnode - num_failed) / float(total_vnodes) * 100)
    # Calculate average time taken to repair 1 vnode
    current_duration = (updated - started).total_seconds()
    avg_vnode_time = current_duration / float(current_vnode - 1)
    return {
        'status': status,
        'nodeposition': node_position,
        'percentage_complete': percentage_complete,
        'current_step_time_seconds': current_step_time,
        'total_repair_time_seconds': total_repair_time,
        'num_failed': num_failed,
        'started': node_status['started'],
        'finished': finished_on,
        'avg_vnode_time_seconds': avg_vnode_time,
        'est_full_repair_time_seconds': avg_vnode_time * NUM_VNODES,
        'est_time_remaining_seconds': avg_vnode_time * (NUM_VNODES - current_vnode),
    }


def write_json(data, file_=sys.stdout):
    """
    Write full repair stats data as JSON.
    
    :param dict data: Repair status data.
    :param file file_: File to write to.
    """
    json.dump(data, file_, indent=4)


def write_summary(data, file_=sys.stdout):
    """
    Write human readable summary.
    
    :param dict data: Repair status data.
    :param file file_: File to write to.
    """
    out = "Fully Repaired    {0}\n" \
          "Repairing         {1}\n" \
          "With Errors       {2}\n" \
          "Hung              {3}\n" \
          "Unknown           {4}\n" \
          "-----------------------\n" \
          "Percent Complete: {5}%\n" \
          "Avg. Vnode Time:  {6:.0f}m\n" \
          "Avg. Host Time:   {7:.0f}m\n" \
          "Est. Full Repair: {8:.0f}m\n".format(
        data['num_fully_repaired'],
        data['num_repairing'],
        data['num_repaired_with_errors'],
        data['num_hung'],
        data['num_no_data'],
        data['percentage_complete'],
        data['avg_vnode_time_seconds'] / 60,
        data['avg_vnode_time_seconds'] * NUM_VNODES / 60,
        data['est_full_repair_time_seconds'] / 60,
    )
    file_.write(out)


def write_csv(data, file_=sys.stdout):
    """
    Print CSV summary.
    
    :param dict data: Repair status data.
    :param file file_: File to write to.
    """
    writer = csv.writer(file_)
    headers = ['Host', 'Is In Progress', 'Has Errors', 'Is Hung', 'Started', 'Finished', 'Duration', 'Current Vnode']
    writer.writerow(headers)
    totals = {
        'in_progress': 0,
        'has_errors': 0,
        'hung': 0,
        'start': datetime.max,
        'end': datetime.min,
        'duration': 0,
        'repaired_vnodes': 0,
    }
    for hostname, host in data['nodes'].iteritems():
        # If node has no data, write an empty row and don't include in any totals
        if host['status'] == STATUS_NO_DATA:
            writer.writerow([hostname, None, None, None, None, None, None, None])
            continue
        # Convert to CSV appropriate data
        in_progress = int(host['status'] == STATUS_REPAIRING)
        has_errors = int(host['num_failed'] > 0)
        is_hung = int(host['status'] == STATUS_HUNG)
        current_vnode = int(host['nodeposition'].split('/')[0])
        started = datetime.strptime(host['started'], '%Y-%m-%dT%H:%M:%S.%f')
        finished = datetime.strptime(host['finished'], '%Y-%m-%dT%H:%M:%S.%f') if host['finished'] else None
        # Calculate totals
        totals['in_progress'] += in_progress
        totals['has_errors'] += has_errors
        totals['hung'] += is_hung
        totals['start'] = min(started, totals['start'])
        totals['end'] = max(finished if finished else datetime.min, totals['end'])
        totals['duration'] += host['total_repair_time_seconds'] or 0
        totals['repaired_vnodes'] += current_vnode
        # Write CSV row
        row = [
            hostname,
            in_progress,
            has_errors,
            is_hung,
            host['started'],
            host['finished'],
            host['total_repair_time_seconds'],
            current_vnode,
        ]
        writer.writerow(row)
    footer = [
        'Total',
        totals['in_progress'],
        totals['has_errors'],
        totals['hung'],
        totals['start'].isoformat(),
        totals['end'].isoformat(),
        totals['duration'],
        '{0} / {1}'.format(totals['repaired_vnodes'], len(data['nodes'].keys()) * NUM_VNODES),
    ]
    writer.writerow(footer)


if __name__ == '__main__':
    parser = ArgumentParser(description='Check range repair status on multiple nodes')
    parser.add_argument('filename',
                        help='Path to range repair status output file')
    parser.add_argument('nodes', nargs='+',
                        help='List of nodes to check repair status on')
    parser.add_argument('--hang-timeout', dest='hang_timeout', default=DEFAULT_HANG_TIMEOUT,
                        help='Timeout in seconds to assume repair has hung')
    parser.add_argument('--format', choices=['summary', 'csv', 'json'], default='json',
                        help='Output format')

    args = parser.parse_args()

    logging.basicConfig()

    # Load SSH
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    user_config_file = os.path.expanduser('~/.ssh/config')
    if os.path.exists(user_config_file):
        with open(user_config_file) as f:
            ssh_config.parse(f)

    cluster = build_cluster(args.nodes, args.filename, args.hang_timeout)

    if args.format == 'summary':
        write_summary(cluster)
    elif args.format == 'csv':
        write_csv(cluster)
    else:
        write_json(cluster)
