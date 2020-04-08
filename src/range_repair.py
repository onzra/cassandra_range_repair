#!/usr/bin/env python
"""
This script will allow for smaller repairs of Cassandra ranges.

Source: https://github.com/onzra/cassandra_range_repair
"""
from __future__ import print_function

from optparse import OptionParser, OptionGroup

import logging
import logging.handlers
import subprocess
import sys
import multiprocessing
import platform
import re
import collections
import time
import six
from datetime import datetime
import json
from multiprocessing.managers import BaseManager
from multiprocessing import Lock
import random

write_status_lock = Lock()

longish = six.integer_types[-1]

ExponentialBackoffRetryerConfig = collections.namedtuple(
    'ExponentialBackoffRetryerConfig', (
        'max_tries',
        'initial_sleep',
        'sleep_factor',
        'max_sleep',
    )
)


def create_key(step, start, end, nodeposition, keyspace, column_families):
    """
    Create key used in status dict to identify a repair.

    :rtype: str
    :return: key string used to identify repair step.
    """
    keyspace = str(keyspace) or '<all>'
    column_families = '<all>' if (column_families == [] or not column_families) else str(column_families)
    key = '{0}_{1}_{2}_{3}_{4}_{5}'.format(step, start, end, nodeposition, keyspace, column_families)
    return key


class ExponentialBackoffRetryer:

    def __init__(self, config, success_checker, executor, sleeper=lambda x: time.sleep(x)):
        """Constructur.

        Params:
        config -- an instance of ExponentialBackoffRetryerConfig.
        success_checker -- a callable that takes the result of the `executor` and returns true if the result was successful, False otherwise.
        executor -- executes something and returns a result.
        sleeper -- a callable that sleeps a number of seconds. Useful to be mocked for testing.
        """
        self.config = config
        self.success_checker = success_checker
        self.executor = executor
        self.sleeper = sleeper

    def __call__(self, *args, **kwargs):
        next_sleep = self.config.initial_sleep
        for i in range(self.config.max_tries):
            result = self.executor(*args, **kwargs)
            if self.success_checker(result):
                return result
            else:
                logging.warning("Execution failed.")
                last_iteration = (i == self.config.max_tries-1)
                if not last_iteration:
                    # Not reason to sleep if we aren't about to retry.
                    logging.info("Sleeping %d seconds until retrying again.", next_sleep)
                    self.sleeper(next_sleep
                                 if self.config.max_sleep <= 0
                                 else min(next_sleep, self.config.max_sleep))
                    next_sleep *= self.config.sleep_factor
                else:
                    logging.warning("Giving up execution. Failed too many times.")

        return result


class TokenContainer:
    'Place to keep tokens'
    RANGE_MIN = -(2**63)
    RANGE_MAX = (2**63)-1
    FORMAT_TEMPLATE = "{0:+021d}"
    def __init__(self, options):
        '''Initialize the Token Container by getting the host and ring tokens and
        then confirming the values used for formatting and range
        management.
        :param options: OptionParser result
        :returns: None
        '''
        self.options = options
        self.local_nodes = []
        self.host_tokens = []
        self.ring_tokens = []
        self.host_token_count = -1
        self.get_local_nodes()
        self.get_host_tokens()
        self.get_ring_tokens()
        self.check_for_MD5_tokens()
        return

    def get_local_nodes(self):
        '''In a multi-DC environment, it is important to *only* consider tokens on
        members of the local ring.

        '''
        if not self.options.datacenter:
            logging.debug("No datacenter specified, all ring members' tokens will be considered")
            return
        logging.debug("Determining local ring members")
        cmd = [self.options.nodetool, "-h", self.options.host, "-p", self.options.port, "gossipinfo"]
        success, _, stdout, stderr = run_command(*cmd)

        if not success:
            raise Exception("Died in get_ring_tokens because: " + stderr)

        # This is a really well-specified value.  If the format of the
        # output of 'nodetool gossipinfo' changes, this will have to be
        # revisited.
        search_regex = "DC(?::\d+)?:{datacenter}".format(datacenter=self.options.datacenter)
        for paragraph in stdout.split("/"):
            if not re.search(search_regex, paragraph):
                continue
            self.local_nodes.append(paragraph.split()[0])
        logging.info("Local nodes: " + " ".join(self.local_nodes))
        return

    def check_for_MD5_tokens(self):
        """By default, the TokenContainer assumes that the Murmur3 partitioner is
        in use.  If that's true, then the first token in the ring should
        have a negative value as long as the cluster has at least 3
        (v)nodes.  If the first token is not negative, switch the class
        constants for the values associated with Random paritioner.
        :returns: None
        """
        if not self.ring_tokens[0] < 0:
            self.FORMAT_TEMPLATE = "{0:039d}"
            self.RANGE_MIN = 0
            self.RANGE_MAX = (2**127) - 1
        return

    def get_ring_tokens(self):
        """Gets the token information for the ring
        :returns: None
        """
        logging.info("running nodetool ring, this will take a little bit of time")
        cmd = [self.options.nodetool, "-h", self.options.host, "-p", self.options.port, "ring"]
        success, _, stdout, stderr = run_command(*cmd)

        if not success:
            raise Exception("Died in get_ring_tokens because: " + stderr)

        logging.debug("ring tokens found, creating ring token list...")
        for line in stdout.split("\n")[4:]:
            segments = line.split()
            # Filter tokens from joining nodes
            if (len(segments) != 8) or (segments[3] == "Joining"):
                logging.debug("Discarding: %s", line)
                continue
            # If a datacenter has been specified, filter nodes that are in
            # different datacenters.
            if self.options.datacenter and not segments[0] in self.local_nodes:
                logging.debug("Discarding node/token %s/%s", segments[0], segments[-1])
                continue
            self.ring_tokens.append(longish(segments[-1]))
            # Excessive logging
            # logging.debug(str(self.ring_tokens))
        self.ring_tokens.sort()
        logging.info("Found {0} tokens".format(len(self.ring_tokens)))
        logging.debug(self.ring_tokens)
        return

    def get_host_tokens(self):
        """Gets the tokens ranges for the target host
        :returns: None
        """
        cmd = [self.options.nodetool, "-h", self.options.host, "-p", self.options.port, "info", "-T"]
        success, _, stdout, stderr = run_command(*cmd)
        if not success or stdout.find("Token") == -1:
            logging.error(stdout)
            raise Exception("Died in get_host_tokens, success: %d, stderr: %s" % (success, stderr))

        for line in stdout.split("\n"):
            if not line.startswith("Token"): continue
            parts = line.split()
            self.host_tokens.append(longish(parts[-1]))
        self.host_tokens.sort()
        self.host_token_count = len(self.host_tokens)
        logging.debug("%d host tokens found", self.host_token_count)
        return

    def format(self, value):
        '''Return the correctly zero-padded string for the token.
        :returns: the properly-formatted token.
        '''
        return self.FORMAT_TEMPLATE.format(value)

    def get_preceding_token(self, token):
        """get the end token of the previous range
        :param token: Reference token
        :returns: The token that falls immediately before the argument token
        """
        for i in reversed(self.ring_tokens):
            if token > i:
                return i
        # token is the smallest value in the ring.  Since the rings wrap around,
        # return the last value.
        return self.ring_tokens[-1]

    def sub_range_generator(self, start, stop, steps=100):
        """Generate $step subranges between $start and $stop
        :param start: beginning token in the range
        :param stop: first token of the next range
        :param steps: number of sub-ranges to create
        :returns: string-formatted start value, string-formatted end value, current step number
        There is special-case handling for when there are more steps than there
        are keys in the range: just return the start and stop values.
        """
        step = 0
        # This first case works for all but the highest-valued token.
        if stop > start:
            if start+steps < stop+1:
                step_increment = ((stop - start) // steps)
                # We would have an extra, tiny step in the event the range
                # is not evenly divisible by the number of steps.  This may
                # give us one larger step at the end.
                step_list = [self.format(x) for x in range(start, stop, step_increment)][0:steps]
            else:
                step += 1
                yield self.format(start), self.format(stop), step
        else:                     # This is the wrap-around case
            distance = (self.RANGE_MAX - start) + (stop - self.RANGE_MIN)
            if distance > steps-1:
                step_increment = distance // steps
                # Can't use xrange here because the numbers are too large!
                step_list = [self.format(x) for x in range(start, self.RANGE_MAX, step_increment)]
                step_list.extend([self.format(x) for x in range(self.RANGE_MIN, stop, step_increment)])
                if len(step_list) > steps-1:
                    step_list.pop()
            else:
                step += 1
                yield self.format(start), self.format(stop), step
        step_list.append(self.format(stop)) # Add the final number to the list
        # Now iterate pair-wise over the list
        while len(step_list) > 1:
            step += 1
            yield step_list[0], step_list[1], step
            step_list.pop(0)


class RepairStatus(object):
    """
    Record repair status and write to a file.
    """

    def __init__(self):
        """
        Init.
        """
        # Repair settings
        self.filename = None
        self.steps = None
        # Timestamps
        self.started = None
        self.updated = None
        self.finished = None
        self.last_resumed_at = None
        # Counters
        self.successful_count = 0
        self.failed_count = 0
        # Repair operations
        self.failed_repairs = {}
        self.current_repairs = {}
        self.finished_repairs = {}
        self.pending_repairs = {}

    def start(self, options):
        """
        Start recording repair status.

        :param options: Range repair options.
        """
        self.filename = options.output_status
        self.steps = options.steps
        self.reset()
        self.started = datetime.now().isoformat()
        self.write()

    def add_pending_repair(self, k, p):
        self.pending_repairs[k] = p

    def gp(self):
        return self.pending_repairs

    def resume(self, options, tokens):
        """
        Resume a hung or canceled range repair.

        :param options: Range repair options.
        :param TokenContainer tokens: Tokens.

        :rtype: int
        :return: Token offset to resume repairs at.
        """
        # Repair settings
        self.filename = options.output_status
        self.steps = options.steps
        # Load existing data from output status file
        f = open(self.filename, 'r')
        status = json.load(f)
        f.close()
        if status['finished']:
            raise Exception('Cannot resume, repair status indicates it has already finished at {0}'
                            .format(status['finished']))
        self._from_output_status(status)
        # Set resumed data
        self.last_resumed_at = datetime.now().isoformat()
        self.write()
        return True

    def reset(self):
        """
        Reset all repair status values.
        """
        self.started = None
        self.updated = None
        self.finished = None
        self.failed_repairs = {}
        self.current_repairs = {}
        self.finished_repairs = {}
        self.pending_repairs = {}
        self.failed_count = 0
        self.successful_count = 0
        self.last_resumed_at = None

    def repair_start(self, cmd, step, start, end, nodeposition, keyspace=None, column_families=None):
        """
        Record when a repair step starts.

        :param cmd: Repair command.
        :param step: Step number.
        :param start: Start range.
        :param end: End range.
        :param nodeposition: Node position.
        :param keyspace: Keyspace being repaired.
        :param column_families: Column families being repaired.
        """
        k = create_key(step, start, end, nodeposition, keyspace, column_families)
        self.current_repairs[k] = self._build_repair_dict(cmd, step, start, end, nodeposition, keyspace, column_families)
        self.write()

    def repair_fail(self, cmd, step, start, end, nodeposition, keyspace=None, column_families=None):
        """
        Record when a repair step fails.

        :param cmd: Repair command.
        :param step: Step number.
        :param start: Start range.
        :param end: End range.
        :param nodeposition: Node position.
        :param keyspace: Keyspace being repaired.
        :param column_families: Column families being repaired.
        """
        k = create_key(step, start, end, nodeposition, keyspace, column_families)
        self.current_repairs[k] = self._build_repair_dict(cmd, step, start, end, nodeposition, keyspace, column_families)
        self.failed_repairs[k] = self.current_repairs[k]
        del self.current_repairs[k]
        del self.pending_repairs[k]

        self.failed_repairs.append(
            self._build_repair_dict(cmd, step, start, end, nodeposition, keyspace, column_families)
        )
        self.failed_count += 1
        self.write()

    def repair_success(self, cmd, step, start, end, nodeposition, keyspace=None, column_families=None):
        """
        Record when a repair step succeeds.

        :param cmd: Repair command.
        :param step: Step number.
        :param start: Start range.
        :param end: End range.
        :param nodeposition: Node position.
        :param keyspace: Keyspace being repaired.
        :param column_families: Column families being repaired.
        """
        k = create_key(step, start, end, nodeposition, keyspace, column_families)
        self.finished_repairs[k] = self.current_repairs[k]
        del self.current_repairs[k]
        del self.pending_repairs[k]
        self.successful_count += 1
        self.write()

    def finish(self):
        """
        Set repair session as finished.
        """
        self.finished = datetime.now().isoformat()
        self.write()

    def write(self):
        """
        Write repair status to file, if requested.
        """
        # No filename indicates output status was not requested

        with write_status_lock:
            if self.filename:
                self.updated = datetime.now().isoformat()
                file = open(self.filename, 'w')
                file.write(json.dumps({
                    'started': self.started,
                    'updated': self.updated,
                    'finished': self.finished,
                    'pending_repairs': self.pending_repairs,
                    'current_repairs': self.current_repairs,
                    'finished_repairs': self.finished_repairs,
                    'failed_repairs': self.failed_repairs,
                    'successful_count': self.successful_count,
                    'failed_count': self.failed_count,
                    'steps': self.steps,
                    'last_resumed_at': self.last_resumed_at,
                }))
                file.close()

    def _from_output_status(self, status):
        """
        Load data from existing output status file.

        :param dict status: Status output data.
        """
        self.started = status['started']
        self.updated = status['updated']
        self.finished = status['finished']
        self.last_resumed_at = status['last_resumed_at']
        self.failed_repairs = status['failed_repairs']
        self.pending_repairs = status['pending_repairs']
        self.current_repairs = status['current_repairs']
        self.finished_repairs = status['finished_repairs']
        self.successful_count = status['successful_count']
        self.failed_count = status['failed_count']

    @staticmethod
    def _build_repair_dict(cmd, step, start, end, nodeposition, keyspace=None, column_families=None):
        """
        Build a standard repair step dict.

        :param cmd: Repair command.
        :param step: Step number.
        :param start: Start range.
        :param end: End range.
        :param nodeposition: Node position.
        :param keyspace: Keyspace being repaired.
        :param column_families: Column families being repaired.

        :rtype: dict
        :return: Dict of repair step info.
        """
        return {
            'time': datetime.now().isoformat(),
            'step': step,
            'start': start,
            'end': end,
            'nodeposition': nodeposition,
            'keyspace': keyspace or '<all>',
            'column_families': column_families or '<all>',
            'cmd': cmd
        }


class TestManager(BaseManager):
    pass
TestManager.register('RepairStatus', RepairStatus)


def run_command(*command):
    """Execute a shell command and return the output
    :param command: the command to be run and all of the arguments
    :returns: success_boolean, command_string, stdout, stderr
    """
    cmd = " ".join(map(str, command))
    logging.debug("run_command: " + cmd)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = proc.communicate()
    return proc.returncode == 0, cmd, stdout, stderr

def repair_range(options, start, end, step, nodeposition, repair_status=None):
    """Repair a keyspace/columnfamily between a given token range with nodetool
    :param options: OptionParser result
    :param start: Beginning token in the range to repair (formatted string)
    :param end: Ending token in the range to repair (formatted string)
    :param step: The step we're executing (for logging purposes)
    :param nodeposition: string to indicate which node this particular step is for.
    :param RepairStatus repair_status: Repair status.
    :returns: None
    """
    if options.exclude_step:
        excluded = is_excluded(options, start, end, step, nodeposition)
        if excluded == 1:
            logging.debug(
                "{nodeposition} step {step:04d} skipping range ({start}, {end}) for keyspace {keyspace}".format(
                    step=step,
                    start=start,
                    end=end,
                    nodeposition=nodeposition,
                    keyspace=options.keyspace or "<all>"))
            return
        elif excluded == 2:
            logging.info(
                'Running individual repair commands for each keyspace to exclude {0} {1}'.format(
                    options.exclude_step['keyspace'],
                    options.exclude_step['column_family'] or ''))
            for keyspace, column_families in enumerate_keyspaces(options).iteritems():
                if keyspace == options.exclude_step['keyspace']:
                    if options.exclude_step['column_family']:
                        logging.info('Repairing all column families except {0} for keyspace {1}'.format(
                            options.exclude_step['column_family'],
                            keyspace))
                        cf_to_repair = [cf for cf in column_families if cf != options.exclude_step['column_family']]
                        _repair_range(options, start, end, step, nodeposition, keyspace, cf_to_repair, repair_status)
                        continue
                    else:
                        logging.debug(
                            "{nodeposition} step {step:04d} skipping range ({start}, {end}) for keyspace {keyspace}".format(
                                step=step,
                                start=start,
                                end=end,
                                nodeposition=nodeposition,
                                keyspace=keyspace))
                        continue
                _repair_range(options, start, end, step, nodeposition, keyspace, options.columnfamily, repair_status)
            return
    # Normal repair_range
    _repair_range(options, start, end, step, nodeposition, options.keyspace, options.columnfamily, repair_status)

def _repair_range(options, start, end, step, nodeposition, keyspace=None, column_families=None, repair_status=None):
    """Repair a keyspace/columnfamily between a given token range with nodetool
    :param options: OptionParser result
    :param start: Beginning token in the range to repair (formatted string)
    :param end: Ending token in the range to repair (formatted string)
    :param step: The step we're executing (for logging purposes)
    :param nodeposition: string to indicate which node this particular step is for.
    :param keyspace: Keyspace to repair.
    :param column_families: List of column families to repair.
    :param RepairStatus repair_status: Repair status.
    :returns: None
    """
    logging.debug(
        "{nodeposition} step {step:04d} repairing range ({start}, {end}) for keyspace {keyspace}".format(
            step=step,
            start=start,
            end=end,
            nodeposition=nodeposition,
            keyspace=keyspace or "<all>"))

    cmd = [options.nodetool, "-h", options.host, "-p", options.port, "repair"]
    if keyspace: cmd.append(keyspace)
    cmd.extend(column_families or options.columnfamily)

    # -local flag cannot be used in conjunction with -pr
    if options.local:
        cmd.extend([options.local])
    else:
        cmd.extend(["-pr"])

    cmd.extend([options.par, options.inc, options.snapshot,
                 "-st", start, "-et", end])
    cmd_str = ' '.join(map(str, cmd))

    if repair_status:
        repair_status.repair_start(cmd_str, step, start, end, nodeposition, keyspace, column_families)

    if not options.dry_run:
        seconds_to_sleep = random.uniform(0, options.max_sleep_before_run)
        logging.info("Sleeping for {0} seconds before run.".format(seconds_to_sleep))
        time.sleep(seconds_to_sleep)

        retry_options = ExponentialBackoffRetryerConfig(options.max_tries, options.initial_sleep,
            options.sleep_factor, options.max_sleep)
        retryer = ExponentialBackoffRetryer(retry_options, lambda x: x[0], run_command)
        success, cmd, _, stderr = retryer(*cmd)
    else:
        print("{step:04d}/{nodeposition}".format(nodeposition=nodeposition, step=step), " ".join([str(x) for x in cmd]))
        success = True
    if not success:
        if repair_status:
            repair_status.repair_fail(cmd_str, step, start, end, nodeposition, keyspace, column_families)
        logging.error("FAILED: {nodeposition} step {step:04d} {cmd}".format(nodeposition=nodeposition, step=step, cmd=cmd))
        logging.error(stderr)
        return
    else:
        if repair_status:
            repair_status.repair_success(cmd_str, step, start, end, nodeposition, keyspace, column_families)
    logging.debug("{nodeposition} step {step:04d} complete".format(nodeposition=nodeposition,step=step))
    return

def setup_logging(option_group):
    """Sets up logging in a syslog format by log level
    :param option_group: options as returned by the OptionParser
    """
    stderr_log_format = "%(levelname) -10s %(asctime)s %(funcName) -20s line:%(lineno) -5d: %(message)s"
    file_log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logger = logging.getLogger()
    if option_group.debug:
        logger.setLevel(level=logging.DEBUG)
    elif option_group.verbose:
        logger.setLevel(level=logging.INFO)
    else:
        logger.setLevel(level=logging.WARNING)

    handlers = []
    if option_group.syslog:
        handlers.append(logging.handlers.SysLogHandler(facility=option_group.syslog))
        # Use standard format here because timestamp and level will be added by syslogd.
    if option_group.logfile:
        handlers.append(logging.FileHandler(option_group.logfile))
        handlers[0].setFormatter(logging.Formatter(file_log_format))
    if not handlers:
        handlers.append(logging.StreamHandler())
        handlers[0].setFormatter(logging.Formatter(stderr_log_format))
    for handler in handlers:
        logger.addHandler(handler)
    return

def repair(options):
    """Repair a keyspace/columnfamily by breaking each token range into $start_steps ranges
    :param options.keyspace: Cassandra keyspace to repair
    :param options.host: (optional) Hostname to pass to nodetool
    :param options.port: (optional) JMX Port to pass to nodetool
    :param options.steps: Number of sub-ranges to split primary range in to
    :param options.workers: Number of workers to use
    """
    tokens = TokenContainer(options)

    worker_pool = multiprocessing.Pool(options.workers)
    manager = TestManager()
    manager.start()
    repair_status = manager.RepairStatus()

    # TODO: Modifying options.resume to use dictionary instead of offset.
    if options.resume:
        options.offset = repair_status.resume(options, tokens)

        all_results = []
        for g in repair_status.gp():
            ga = repair_status.gp()[g]
            nodeposition = ga['nodeposition']
            start = ga['start']
            end = ga['end']
            step = ga['step']
            args = (options, start, end, step, nodeposition, repair_status)
            all_results.append(worker_pool.apply_async(repair_range, args))

        for r in list(all_results):
            r.get()

        repair_status.finish()
        return
    else:
        repair_status.start(options)

    # Store all results in one large list to prevent throttling by discrete step size.
    all_results = []

    for token_num, host_token in enumerate(tokens.host_tokens):
        range_termination = host_token
        range_start = tokens.get_preceding_token(range_termination)

        if token_num < options.offset:
            logging.info(
                "[{count}/{total}] skipping token..".format(
                    count=token_num + 1,
                    total=tokens.host_token_count))
            continue

        # TODO: Nice to have this outside of this loop so it is less confusing (this doesn't start within loop anymore.)
        # logging.info(
        #     "[{count}/{total}] repairing range ({token}, {termination}) in {steps} steps for keyspace {keyspace}".format(
        #         count=token_num + 1,
        #         total=tokens.host_token_count,
        #         token=tokens.format(range_start),
        #         termination=tokens.format(range_termination),
        #         steps=options.steps,
        #         keyspace=options.keyspace or "<all>"))

        results = []
        for start, end, step in tokens.sub_range_generator(range_start, range_termination, options.steps):
            nodeposition = "{count}/{total}".format(count=token_num + 1, total=tokens.host_token_count)
            args = (options, start, end, step, nodeposition, repair_status)
            results.append(worker_pool.apply_async(repair_range, args))

            # TODO: Confirm that the <all> value in this is used correctly in all cases.
            if options.columnfamily:
                column_families = str(options.columnfamily)
            else:
                column_families = '<all>'
            k = create_key(step, start, end, nodeposition, str(options.keyspace), column_families)
            pending_repair = RepairStatus._build_repair_dict(
                '', step, start, end, nodeposition, str(options.keyspace), column_families)
            repair_status.add_pending_repair(k, pending_repair)

        all_results += results

    for r in list(all_results):
        r.get()

    repair_status.finish()
    return

# Exclude Step Feature

def is_excluded(options, start, end, step, nodeposition):
    """Test if a particular range is excluded.
    :param options: OptionParser result
    :param start: Beginning token in the range to repair (formatted string)
    :param end: Ending token in the range to repair (formatted string)
    :param step: The step we're executing (for logging purposes)
    :param nodeposition: string to indicate which node this particular step is for.
    :returns: 0 if not excluded, 1 if entire step is excluded, 2 if only keyspace is excluded.
    """
    current_node = nodeposition.split('/')[0]
    if options.exclude_step['node'] == current_node and options.exclude_step['step'] == step:
        if options.exclude_step['keyspace']:
            if options.keyspace and options.keyspace == options.exclude_step['keyspace']:
                return 1
            elif not options.keyspace:
                # No options.keyspace means all keyspaces, but we only want to exclude one keyspace
                return 2
        else:
            return 1
    return 0

def enumerate_keyspaces(options):
    """Get a dict of all keyspaces and their column families.
    :param options: OptionParser result
    :returns: Dictionary of keyspace: [column families]
    """
    logging.info('running nodetool cfstats')
    cmd = [options.nodetool, "-h", options.host, "-p", options.port, "cfstats"]
    success, _, stdout, stderr = run_command(*cmd)

    if not success:
        raise Exception("Died in enumerate_keyspaces because: " + stderr)

    logging.debug('cfstats retrieved, parsing output to retrieve keyspaces')
    # Build a dictionary of keyspace: [column families]
    keyspaces = {}
    keyspace = None
    for line in stdout.split("\n"):
        if line.startswith('Keyspace: '):
            keyspace = line.replace('Keyspace: ', '')
            keyspaces[keyspace] = []
        elif line.startswith("\t\tTable: "):
            table = line.replace("\t\tTable: ", '')
            keyspaces[keyspace].append(table)
    logging.info('Found {0} keyspaces'.format(len(keyspaces)))
    # logging.debug(keyspaces)
    return keyspaces

def parse_exclude_step(option, opt_str, value, parser):
    """Parse exclude_step arg.
    :param option: Option instance.
    :param opt_str: Option string.
    :param value: Option value.
    :param parser: Option parser.
    :return: Exclude step value.
    """
    pieces = value.split(',')
    if len(pieces) == 4:
        exclude_step = {
            'keyspace': pieces[0],
            'column_family': pieces[1],
            'node': pieces[2],
            'step': int(pieces[3])
        }
    elif len(pieces) == 3:
        exclude_step = {
            'keyspace': pieces[0],
            'column_family': None,
            'node': pieces[1],
            'step': int(pieces[2])
        }
    else:
        exclude_step = {
            'keyspace': None,
            'column_family': None,
            'node': pieces[0],
            'step': int(pieces[1])
        }
    setattr(parser.values, option.dest, exclude_step)

def main():
    """Validate arguments and initiate repair
    """
    parser = OptionParser()
    parser.add_option("-k", "--keyspace", dest="keyspace", metavar="KEYSPACE",
                      help="Keyspace to repair (REQUIRED)")

    parser.add_option("-c", "--columnfamily", dest="columnfamily", default=[],
                      action="append", metavar="COLUMNFAMILY",
                      help="ColumnFamily to repair, can appear multiple times")

    parser.add_option("-H", "--host", dest="host", default=platform.node(),
                      metavar="HOST", help="Hostname to repair [default: %default]")

    parser.add_option("-P", "--port", dest="port", default=7199, type="int",
                      metavar="PORT", help="JMX port to use for nodetool commands [default: %default]")

    parser.add_option("-s", "--steps", dest="steps", type="int", default=100,
                      metavar="STEPS", help="Number of discrete ranges [default: %default]")

    parser.add_option("-o", "--offset", dest="offset", type="int", default=0,
                      metavar="OFFSET", help="Number of tokens to skip [default: %default]")

    parser.add_option("-n", "--nodetool", dest="nodetool", default="nodetool",
                      metavar="NODETOOL", help="Path to nodetool [default: %default]")

    # The module default for workers is actually the CPU count, but we're
    # going to override it to 1, which matches the old behavior of serial
    # repairs.
    parser.add_option("-w", "--workers", dest="workers", type="int", default=1,
                      metavar="WORKERS", help="Number of workers to use for parallelism [default: %default]")

    parser.add_option("-D", "--datacenter", dest="datacenter", default=None,
                      metavar="DATACENTER", help="Identify local datacenter [default: %default]")

    parser.add_option("-l", "--local", dest="local", default="",
                      action="store_const", const="-local",
                      metavar="LOCAL", help="Restrict repair to the local DC")

    parser.add_option("-p", "--par", dest="par", default="",
                      action="store_const", const="-par",
                      metavar="PAR", help="Carry out a parallel repair (post-2.x only)")

    parser.add_option("-i", "--inc", dest="inc", default="",
                      action="store_const", const="-inc",
                      metavar="INC", help="Carry out an incremental repair (post-2.1 only). Forces --par")

    parser.add_option("-S", "--snapshot", dest="snapshot", default="",
                      action="store_const", const="-snapshot",
                      metavar="LOCAL", help="Use snapshots (pre-2.x only)")

    parser.add_option("-v", "--verbose", dest="verbose", action='store_true',
                      default=False, help="Verbose output")

    parser.add_option("-d", "--debug", dest="debug", action='store_true',
                      default=False, help="Debugging output")

    parser.add_option("--dry-run", dest="dry_run", action='store_true',
                      default=False, help="Do not execute repairs.")

    parser.add_option("--syslog", dest="syslog", metavar="FACILITY",
                      help="Send log messages to the syslog")

    parser.add_option("--logfile", dest="logfile", metavar="FILENAME",
                      help="Send log messages to a file")

    parser.add_option("--exclude-step", dest="exclude_step", action="callback", type="str",
                      help="Exclude a [keyspace,[column_family,]]node,step in repairs", callback=parse_exclude_step)

    parser.add_option("--output-status", dest="output_status",
                      help="Output (and update) a status file for each run")

    parser.add_option("--resume", dest="resume", action='store_true', default=False,
                      help="Resume a hung or canceled repair session, requires an existing --output-status file")

    parser.add_option("--max-sleep-before-run", dest="max_sleep_before_run", default=60,
                      help="Maximum number of random seconds to sleep before the next execution.")

    expBackoffGroup = OptionGroup(parser, "Exponential backoff options",
                                  "Every failed `nodetool repair` call can be retried using exponential backoff."
                                  " This is useful if you have flaky connectivity between datacenters.")

    expBackoffGroup.add_option("--max-tries", dest="max_tries", type="int", metavar="N", default=1,
                               help="Number of times to rerun a failed `nodetool repair` call [default: %default]")

    expBackoffGroup.add_option("--initial-sleep", dest="initial_sleep", type="float", metavar="SECONDS", default=1,
                               help="Number of seconds to sleep first `nodetool repair` [default: %default]")

    expBackoffGroup.add_option("--sleep-factor", dest="sleep_factor", type="float", metavar="N", default=2,
                               help=("Multiplication factor that sleep time increases with for every failed"
                                     " `nodetool repair` call [default: %default]"))

    expBackoffGroup.add_option("--max-sleep", dest="max_sleep", type="float", metavar="N", default=1800,
                               help=("Maximum time in seconds the retryer is allowed to sleep. Set to zero or"
                                     " negative to disable. [default: %default]"))

    parser.add_option_group(expBackoffGroup)

    (options, args) = parser.parse_args()

    setup_logging(options)

    if options.columnfamily and not options.keyspace: # keyspace is a *required* for columfamilies
        parser.print_help()
        logging.debug('Invalid configuration options')
        sys.exit(1)

    if args:                    # There are no positional parameters
        parser.print_help()
        logging.debug('Extra parameters')
        sys.exit(1)

    if options.inc and not options.par:
        logging.info('Incremental repairs needs --par: enabling')
        options.par = '-par'

    if options.resume and not options.output_status:
        parser.print_help()
        logging.debug('--resume requires --output-status')
        sys.exit(1)

    repair(options)
    exit(0)

if __name__ == "__main__":
    main()
