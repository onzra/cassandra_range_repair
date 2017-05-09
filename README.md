range_repair.py [![build](https://travis-ci.org/BrianGallew/cassandra_range_repair.svg?branch=master)](https://travis-ci.org/BrianGallew/cassandra)
======================

A Python script to repair the primary range of a Cassandra node in N discrete steps [using best practices](http://www.datastax.com/dev/blog/advanced-repair-techniques).

### Background
When Cassandra begins the repair process it constructs a [merkle tree](http://en.wikipedia.org/wiki/Merkle_tree), which is a tree of hashes over segments of data that the node is responsible for. The node compares it's tree to that of the replicas, if there is a difference in the hash values for any of the nodes then the segment for that hash is requested from the replica and is re-inserted.

By default the Merkle tree for Cassandra represents 15 discreet segments, which means that the data on the node is broken into 15 pieces. If the data for one of these 15 pieces is different from that of the replicas then it will result in 1/15th of the data being streamed and re-inserted. This can cause problems for two use cases: dense nodes, and DSE Solr nodes running at or near capacity.

#### Solr Nodes
For DSE Solr nodes when the data transferred as the result of a repair is re-inserted it is also re-indexed because each host maintains it's own independent set of indexes. If the node is already at or near capacity then the additional strain caused by the repair/re-index can push it over the edge. As a last result Cassandra will shed load by dropping mutations. If a mutation is dropped the data will at some point need to be brought over from the replicas through the repair process which unfortunately begins a never ending cycle of re-index/repair.

#### Dense Nodes
For clusters that have a large amount of data per node the repair process could require an unacceptably large amount of data to be streamed and re-inserted.

### How the script works
The script works by figuring out the primary range for the node that it's being executed on, and instead of running repair on the entire range, run the repair on only a smaller sub-range. When a repair is initiated on a sub-range Cassandra constructs a merkle tree only for the range specified, which in turn divides the much smaller range into 15 segments. If there is disagreement in any of the hash values then a much smaller portion of data needs to be transferred which lessens load on the system.

### Multiple Datacenters
If you have multiple datacenters in your ring, then you MUST specify the name of the datacenter containing the node you are repairing as part of the command-line options (--datacenter=DCNAME).  Failure to do so will result in only a subset of your data being repaired (approximately data/number-of-datacenters).  This is because nodetool has no way to determine the relevant DC on its own, which in turn means it will use the tokens from every ring member in every datacenter.

### Options

```
Usage: range_repair.py [options]

Options:
  -h, --help            show this help message and exit
  -k KEYSPACE, --keyspace=KEYSPACE
                        Keyspace to repair (REQUIRED)
  -c COLUMNFAMILY, --columnfamily=COLUMNFAMILY
                        ColumnFamily to repair, can appear multiple times
  -H HOST, --host=HOST  Hostname to repair [default: $HOSTNAME]
  -P PORT, --port=Port  JMX port to use for nodetool [default: 7199]
  -s STEPS, --steps=STEPS
                        Number of discrete ranges [default: 100]
  -o OFFSET, --offset=OFFSET
                        Number of tokens to skip [default: 0]   
  -n NODETOOL, --nodetool=NODETOOL
                        Path to nodetool [default: nodetool]
  -w WORKERS, --workers=WORKERS
                        Number of workers to use for parallelism [default: 1]
  -D DATACENTER, --datacenter=DATACENTER
  -l, --local           Restrict repair to the local DC
  -p, --par             Carry out a parallel repair (post-2.x only)
  -i, --inc             Carry out an incremental repair (post-2.1 only).
  -S, --snapshot        Use snapshots (pre-2.x only)
  -v, --verbose         Verbose output
  -d, --debug           Debugging output
  --dry-run             Do not execute repairs.
  --syslog=FACILITY     Send log messages to the syslog
  --logfile=FILENAME    Send log messages to a file
  --exclude-step=[keyspace,[column_family,]],node,step
                        Exclude a specific step in the repair process, keyspace and column_family are optional
  --output-status=FILENAME
                        Write current repair run status to a file as JSON.
  --resume              Resume a hung or canceled repair session, requires an existing --output-status file
```

### Sample

```
$ ./range_repair.py -k demo_keyspace
INFO       2014-05-09 17:31:33,503    get_ring_tokens                 66  : running nodetool ring, this will take a little bit of time
DEBUG      2014-05-09 17:31:39,057    get_ring_tokens                 72  : ring tokens found, creating ring token list...
DEBUG      2014-05-09 17:31:40,207    get_host_tokens                 86  : host tokens found, creating host token list...
DEBUG      2014-05-09 17:31:40,208    repair_keyspace                 170 : repair over range (-2974082934175371230, -2971948823734978979] with 100 steps for keyspace demo_keyspace
DEBUG      2014-05-09 17:31:40,208    repair_keyspace                 176 : step 0100 repairing range (-2974082934175371230, -2974061593070967308] for keyspace demo_keyspace ...
DEBUG      2014-05-09 17:32:47,508    repair_keyspace                 182 : SUCCESS
DEBUG      2014-05-09 17:32:47,509    repair_keyspace                 176 : step 0099 repairing range (-2974061593070967308, -2974040251966563386] for keyspace demo_keyspace ...
DEBUG      2014-05-09 17:33:54,904    repair_keyspace                 182 : SUCCESS
...
```

### Resume

If you are using --output-status you can resume a hung or canceled range repair session at the last completed token 
using the --resume option. You must provide the exact same options as the original command, adding only --resume. This 
will adjust the starting offset resume repairs where they left off.

Example:

    $ ./range_repair.py -H localhost -k test -s 1 --output-status status.json
    0001/1/256 nodetool -h localhost -p 7199 repair test -pr    -st -09199621222225968109 -et -09197526493222166483
    ...
    0001/151/256 nodetool -h localhost -p 7199 repair test -pr    -st +01989896843880866331 -et +01995383507845825326
    ^C
    $ ./range_repair.py -H localhost -k test -s 1 --output-status status.json --resume
    0001/151/256 nodetool -h localhost -p 7199 repair test -pr    -st +01989896843880866331 -et +01995383507845825326

### Dependencies
-   Python 2.7+
-   six
-   Cassandra ```nodetool``` must exist in the ```PATH```

## How To Test/Contribute
1. You will need pbr (https://pypi.python.org/pypi/pbr) available in your $PYTHONPATH
1. Go to the root of the cassandra_range_repair project on your local Git clone.
2. Make the necessary changes you want included
3. If you are testing the package on Unix/Linux based environment, make sure the package builds and tests ok

```
make clean
make build
make test
make debian
```

4. Create a pull request to merge to the main branch

### History
- Originally by [Matt Stump](https://github.com/mstump)
- Converted to work with vnodes by [Brian Gallew](https://github.com/BrianGallew)
- Additional functionality by [Eric Lubow](http://github.com/elubow)
- Support for multiprocessing performed by [Brian Gallew](https://github.com/BrianGallew) with credit to [M. Jaszczyk](https://github.com/mjaszczyk)
- Multiple datacenter support by [Brian Gallew](https://github.com/BrianGallew)
- Support debian packaging [Venkatesh Kaushik](https://github.com/higgsmass)
- Exclude step & output status [David Mertl](https://github.com/dmertl)
- Resume support [David Mertl](https://github.com/dmertl)

### TODO

- Resume only supports resuming at a token, support resuming at a step.
- There is a lot of potential for conflict when using --exclude-step and --output-status or --resume. Maybe these 
  should be mutually exclusive?
