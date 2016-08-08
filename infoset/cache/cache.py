#!/usr/bin/env python3

"""Demonstration Script that extracts agent data from cache directory files.

This could be a modified to be a daemon

"""

# Standard libraries
import os
import time
import shutil
from collections import defaultdict
import queue as Queue
import threading
import re

# Infoset libraries
from infoset.db import db
from infoset.db import db_agent as agent
from infoset.utils import log
from infoset.cache import drain

# Define a key global variable
THREAD_QUEUE = Queue.Queue()


class FillDB(threading.Thread):
    """Threaded polling.

    Graciously modified from:
    http://www.ibm.com/developerworks/aix/library/au-threadingpython/

    """

    def __init__(self, queue):
        """Initialize the threads."""
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        """Update the database using threads."""
        while True:
            # Get the data_dict
            data_dict = self.queue.get()
            uid = data_dict['uid']
            metadata = data_dict['metadata']
            config = data_dict['config']
            agents = data_dict['agents']
            datapoints = data_dict['datapoints']

            # Initialize other values
            max_timestamp = 0

            # Sort metadata by timestamp
            metadata.sort()

            # Process file for each timestamp
            for (timestamp, filepath) in metadata:
                # Read in data
                ingest = drain.Drain(filepath)

                # Make sure file is OK
                # Move it to a directory for further analysis
                # by administrators
                if ingest.valid() is False:
                    log_message = (
                        'Cache ingest file %s is invalid. Moving.'
                        '') % (filepath)
                    log.log2warn(1054, log_message)
                    shutil.move(
                        filepath, config.ingest_failures_directory())
                    continue

                # Update agent table if not there
                if ingest.uid() not in agents:
                    _insert_agent(
                        ingest.uid(),
                        ingest.agent(),
                        ingest.hostname(),
                        config
                        )
                    # Append the new insertion to the list
                    agents.append(ingest.uid())

                # Update datapoint metadata if not there
                for item in ingest.sources():
                    did = item[1]
                    if did not in datapoints:
                        _insert_datapoint(item, config)
                        # Append the new insertion to the list
                        datapoints.append(did)

                # Create map of DIDs to database row index values
                mapping = _datapoints_by_did(config)

                # Update chartable data
                _update_chartable(mapping, ingest, config)
                _update_unchartable(mapping, ingest, config)

                # Get the max timestamp
                max_timestamp = max(timestamp, max_timestamp)

                # Purge source file
                ingest.purge()

            # Update the last time the agent was contacted
            _update_agent_last_update(uid, max_timestamp, config)

            # All done!
            self.queue.task_done()


def _update_chartable(mapping, ingest, config):
    """Insert data into the database "iset_data" table.

    Args:
        mapping: Map of DIDs to database row index values
        ingest: Drain object
        config: Config object

    Returns:
        None

    """
    # Initialize key variables
    data = ingest.chartable()
    data_list = []
    timestamp_tracker = {}

    # Update data
    for item in data:
        # Process each datapoint item found
        (_, did, tuple_value, timestamp) = item
        idx_datapoint = int(mapping[did][0])
        idx_agent = int(mapping[did][1])
        last_timestamp = int(mapping[did][2])
        value = float(tuple_value)

        # Only update with data collected after
        # the most recent update. Don't do anything more
        if timestamp > last_timestamp:
            data_list.append(
                (idx_datapoint, idx_agent, value, timestamp)
            )

            # Update DID's last updated timestamp
            if idx_datapoint in timestamp_tracker:
                timestamp_tracker[idx_datapoint] = max(
                    timestamp, timestamp_tracker[idx_datapoint])
            else:
                timestamp_tracker[idx_datapoint] = timestamp

    # Update if there is data
    if bool(data_list) is True:
        # Prepare SQL query to read a record from the database.
        sql_insert = (
            'REPLACE INTO iset_data '
            '(idx_datapoint, idx_agent, value, timestamp) VALUES '
            '(%s, %s, %s, %s)')

        # Do query and get results
        database = db.Database(config)
        database.modify(sql_insert, 1056, data_list=data_list)

        # Change the last updated timestamp
        for idx_datapoint, last_timestamp in timestamp_tracker.items():
            # Prepare SQL query to read a record from the database.
            sql_modify = (
                'UPDATE iset_datapoint SET last_timestamp=%s '
                'WHERE iset_datapoint.idx=%s'
                '') % (last_timestamp, idx_datapoint)
            database.modify(sql_modify, 1057)

        # Report success
        log_message = (
            'Successful cache drain for UID %s at timestamp %s') % (
                ingest.uid(), ingest.timestamp())
        log.log2quiet(1058, log_message)


def _update_unchartable(mapping, ingest, config):
    """Update unchartable data into the database "iset_datapoint" table.

    Args:
        mapping: Map of DIDs to database row index values
        ingest: Drain object
        config: Config object

    Returns:
        None

    """
    # Initialize key variables
    data = ingest.other()
    data_list = []
    timestamp_tracker = {}

    # Update data
    for item in data:
        # Process each datapoint item found
        (_, did, tuple_value, timestamp) = item
        idx_datapoint = int(mapping[did][0])
        last_timestamp = int(mapping[did][2])
        value = ('%s') % (tuple_value)

        # Only update with data collected after
        # the most recent update. Don't do anything more
        if timestamp > last_timestamp:
            data_list.append(
                (idx_datapoint, value)
            )

            # Update DID's last updated timestamp
            if idx_datapoint in timestamp_tracker:
                timestamp_tracker[idx_datapoint] = max(
                    timestamp, timestamp_tracker[idx_datapoint])
            else:
                timestamp_tracker[idx_datapoint] = timestamp

    # Update if there is data
    if bool(data_list) is True:
        for item in data_list:
            (idx_datapoint, value) = item
            fixed_value = str(value)[0:128]

            # Prepare SQL query to read a record from the database.
            sql_modify = (
                'UPDATE iset_datapoint set uncharted_value="%s" WHERE '
                'idx=%s') % (fixed_value, idx_datapoint)

            # Do query and get results
            database = db.Database(config)
            database.modify(sql_modify, 1037)

        # Change the last updated timestamp
        for idx_datapoint, last_timestamp in timestamp_tracker.items():
            # Prepare SQL query to read a record from the database.
            sql_modify = (
                'UPDATE iset_datapoint SET last_timestamp=%s '
                'WHERE iset_datapoint.idx=%s'
                '') % (last_timestamp, idx_datapoint)
            database.modify(sql_modify, 1044)

        # Report success
        log_message = (
            'Successful cache drain (Uncharted Data) '
            'for UID %s at timestamp %s') % (
                ingest.uid(), ingest.timestamp())
        log.log2quiet(1045, log_message)


def _update_agent_last_update(uid, last_timestamp, config):
    """Insert new datapoint into database.

    Args:
        uid: UID of agent
        last_timestamp: The last time a DID for the agent was updated
            in the database
        config: Config object

    Returns:
        None

    """
    # Initialize key variables
    sql_modify = (
        'UPDATE iset_agent SET iset_agent.last_timestamp=%s '
        'WHERE iset_agent.id="%s"'
        '') % (last_timestamp, uid)
    database = db.Database(config)
    database.modify(sql_modify, 1055)


def _insert_datapoint(metadata, config):
    """Insert new datapoint into database.

    Args:
        metadata: Tuple of datapoint metadata.
            (uid, did, label, source, description)
            uid: Agent UID
            did: Datapoint ID
            label: Datapoint label created by agent
            source: Source of the data (subsystem being tracked)
            description: Description provided by agent config file (unused)
            base_type = SNMP base type (Counter32, Counter64, Gauge etc.)
        config: Configuration object

    Returns:
        None

    """
    # Initialize key variables
    (uid, did, label, source, _, base_type) = metadata

    # Get agent index value
    agent_object = agent.Get(uid, config)
    idx_agent = agent_object.idx()

    # Prepare SQL query to read a record from the database.
    sql_query = (
        'INSERT INTO iset_datapoint '
        '(id, idx_agent, agent_label, agent_source, base_type ) VALUES '
        '("%s", %d, "%s", "%s", %d)'
        '') % (did, idx_agent, label, source, base_type)

    # Do query and get results
    database = db.Database(config)
    database.modify(sql_query, 1032)


def _insert_agent(uid, name, hostname, config):
    """Insert new agent into database.

    Args:
        uid: Agent uid
        name: Agent name
        Hostname: Hostname the agent gets data from
        config: Configuration object

    Returns:
        None

    """
    # Prepare SQL query to read a record from the database.
    sql_query = (
        'INSERT INTO iset_agent (id, name, hostname) '
        'VALUES ("%s", "%s", "%s")'
        '') % (uid, name, hostname)

    # Do query and get results
    database = db.Database(config)
    database.modify(sql_query, 1033)


def _datapoints(config):
    """Create list of enabled datapoints.

    Args:
        config: Configuration object

    Returns:
        data: List of active datapoints

    """
    # Initialize key variables
    data = []

    # Prepare SQL query to read a record from the database.
    sql_query = (
        'SELECT iset_datapoint.id '
        'FROM iset_datapoint WHERE (iset_datapoint.enabled=1)')

    # Do query and get results
    database = db.Database(config)
    query_results = database.query(sql_query, 1034)

    # Massage data
    for row in query_results:
        data.append(row[0])

    # Return
    return data


def _datapoints_by_did(config):
    """Create dict of enabled datapoints and their corresponding indices.

    Args:
        config: Configuration object

    Returns:
        data: Dict keyed by datapoint ID,
            with a tuple as its value (idx, idx_agent)
            idx: Datapoint index
            idx_agent: Agent index
            last_timestamp: The last time the timestamp was updated

    """
    # Initialize key variables
    data = {}

    # Prepare SQL query to read a record from the database.
    sql_query = (
        'SELECT iset_datapoint.id, iset_datapoint.idx, '
        'iset_datapoint.idx_agent, iset_datapoint.last_timestamp '
        'FROM iset_datapoint WHERE (iset_datapoint.enabled=1)')

    # Do query and get results
    database = db.Database(config)
    query_results = database.query(sql_query, 1035)

    # Massage data
    for row in query_results:
        did = row[0]
        idx = row[1]
        idx_agent = row[2]
        last_timestamp = row[3]
        data[did] = (idx, idx_agent, last_timestamp)

    # Return
    return data


def _agents(config):
    """Create list of active agent UIDs.

    Args:
        config: Configuration object

    Returns:
        data: List of active agents

    """
    # Initialize key variables
    data = []

    # Prepare SQL query to read a record from the database.
    sql_query = (
        'SELECT iset_agent.id '
        'FROM iset_agent WHERE (iset_agent.enabled=1)')

    # Do query and get results
    database = db.Database(config)
    query_results = database.query(sql_query, 1036)

    # Massage data
    for row in query_results:
        data.append(row[0])

    # Return
    return data


def process(config):
    """Method initializing the class.

    Args:
        config: Configuration object

    Returns:
        None

    """
    # Initialize key variables
    threads_in_pool = config.ingest_threads()
    uid_metadata = defaultdict(lambda: defaultdict(dict))
    cache_dir = config.ingest_cache_directory()

    # Filenames must start with a numeric timestamp and #
    # end with a hex string. This will be tested later
    regex = re.compile(r'^\d+_[0-9a-f]+.json')

    # Get a list of active agents and datapoints
    agents = _agents(config)
    datapoints = _datapoints(config)

    # Spawn a pool of threads, and pass them queue instance
    for _ in range(threads_in_pool):
        update_thread = FillDB(THREAD_QUEUE)
        update_thread.daemon = True
        update_thread.start()

    # Add files in cache directory to list
    all_filenames = [filename for filename in os.listdir(
        cache_dir) if os.path.isfile(
            os.path.join(cache_dir, filename))]

    ######################################################################
    # Create threads
    ######################################################################

    # Process only valid agent filenames
    for filename in all_filenames:
        # Add valid data to lists
        if bool(regex.match(filename)) is True:
            # Create a complete filepath
            filepath = os.path.join(cache_dir, filename)

            # Only read files that are 15 seconds or older
            # to prevent corruption caused by reading a file that could be
            # updating simultaneously
            if time.time() - os.path.getmtime(filepath) < 15:
                continue

            # Create a dict of UIDs, timestamps and filepaths
            (name, _) = filename.split('.')
            (tstamp, uid) = name.split('_')
            timestamp = int(tstamp)
            if uid in uid_metadata:
                uid_metadata[uid].append(
                    (timestamp, filepath))
            else:
                uid_metadata[uid] = [(timestamp, filepath)]

    # Read each cache file
    for uid in uid_metadata.keys():

        ####################################################################
        #
        # Define variables that will be required for the threading
        # We have to initialize the dict during every loop to prevent
        # data corruption
        #
        ####################################################################
        data_dict = {}
        data_dict['uid'] = uid
        data_dict['metadata'] = uid_metadata[uid]
        data_dict['config'] = config
        data_dict['agents'] = agents
        data_dict['datapoints'] = datapoints
        THREAD_QUEUE.put(data_dict)

    # Wait on the queue until everything has been processed
    THREAD_QUEUE.join()

    # PYTHON BUG. Join can occur while threads are still shutting down.
    # This can create spurious "Exception in thread (most likely raised
    # during interpreter shutdown)" errors.
    # The "time.sleep(1)" adds a delay to make sure things really terminate
    # properly. This seems to be an issue on virtual machines in Dev only
    time.sleep(1)
