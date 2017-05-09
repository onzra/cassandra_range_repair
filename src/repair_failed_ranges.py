#!/usr/bin/env python
"""
This script will re-run any failed repairs from a range_repair.py status output file.

Example:

    range_repair.py -k keyspace1 -s 1 --output-status=output.json

    repair_failed_ranges.py output.json

It will also update the output.json file with new counts and timestamps. Any repairs that fail again will stay in the
failed_repairs list.
"""
import json
import logging
import subprocess
from argparse import ArgumentParser
from datetime import datetime


def repair_failed_ranges(status, filename):
    """
    Repair failed ranges given a range repair's status output object.

    Status output object looks like:

        {
            "current_repair": {
                "cmd": "nodetool -h localhost -p 7199 repair cisco_test -pr -st +09176475922996267832 ...",
                "column_families": "<all>",
                "end": "+09188223637297142667",
                "keyspace": "cisco_test",
                "nodeposition": "256/256",
                "start": "+09176475922996267832",
                "step": 1,
                "time": "2017-04-26T03:44:42.543667"
            },
            "failed_count": 5,
            "failed_repairs": [
                {
                    "cmd": "nodetool -h localhost -p 7199 repair cisco_test -pr -st -08956690834811572306 ...".
                    "column_families": "<all>",
                    "end": "-08935863217669227885",
                    "keyspace": "cisco_test",
                    "nodeposition": "5/256",
                    "start": "-08956690834811572306",
                    "step": 1,
                    "time": "2017-04-26T03:44:41.562615"
                },
            ],
            "finished": "2017-04-26T00:00:00.000000",
            "started": "2017-04-26T00:00:00.000000",
            "successful_count": 251,
            "updated": "2017-04-26T00:00:00.000000"
        }

    :param dict status: Status output object.
    :param str filename: Filename to write status to.

    :rtype: int
    :return: Number of repairs that failed again
    """
    if len(status['failed_repairs']) > 0:
        # Clear the finished timestamp
        status['finished'] = None
        write_status(status, filename)
        logging.info('> Attempting to repair {0} failed ranges'.format(len(status['failed_repairs'])))

        # Make a copy of failed repairs list we can loop through
        failed_repairs = [i.copy() for i in status['failed_repairs']]
        # Pointer to current position in status object, we'll be modifying its list
        for failed_repair in failed_repairs:

            # Remove the failed repair from status object as we're about to attempt it again
            # This is basically resetting everything as if we had not attempted this repair yet
            del status['failed_repairs'][0]
            status['failed_count'] -= 1

            # Update status object with new repair run
            failed_repair['time'] = datetime.now().isoformat()
            status['current_repair'] = failed_repair
            write_status(status, filename)

            # Run exact same repair command
            success, stdout, stderr = run_command(failed_repair['cmd'])

            if success:
                # Failed repair was already removed from status object, so just update success count and timestamp
                status['successful_count'] += 1
                logging.info('Successfully repaired {0}'.format(failed_repair['cmd']))
            else:
                # Add failed repair back into status object
                status['failed_count'] += 1
                status['failed_repairs'].append(failed_repair)
                logging.error('Failed again to repair {0}'.format(failed_repair['cmd']))
                logging.error('{0}'.format(stderr))
            write_status(status, filename)
        status['finished'] = datetime.now().isoformat()
        write_status(status, filename)
        logging.info('Finished repairing failed ranges')
    else:
        logging.info('No failed repair ranges to run')
    return len(status['failed_repairs'])


def run_command(cmd):
    """
    Run a repair command.

    :param str cmd: Repair command.

    :rtype: tuple
    :return: success, stdout, stderr
    """
    logging.info('run_command: {0}'.format(cmd))
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = proc.communicate()
    return proc.returncode == 0, stdout, stderr


def write_status(status, filename):
    """
    Write status object to a file.

    :param dict status: Status object.
    :param str filename: Filename to write to.
    """
    status['updated'] = datetime.now().isoformat()
    file = open(filename, 'w')
    file.write(json.dumps(status))
    file.close()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('filename')
    args = parser.parse_args()

    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(level=logging.INFO)

    f = open(args.filename, 'r')
    status = json.load(f)
    f.close()

    num_failed = repair_failed_ranges(status, args.filename)

    # Exit code indicates number of repairs that failed again
    exit(num_failed)
