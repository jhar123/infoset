#!/usr/bin/env python3
"""Classes for polling remote hosts for SNMP data."""

import tempfile
import time
import os
import queue as Queue
import threading


from infoset.utils import jm_general
from infoset.snmp import snmp_manager
from infoset.snmp import snmp_info


# Define a key global variable
THREAD_QUEUE = Queue.Queue()


class PollOids(threading.Thread):
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

            # Signals to queue job is done
            self.queue.task_done()


def snmp(config, verbose=False):
    """Process 'poll' CLI option.

    Args:
        config: Configuration object
        verbose: Verbose output if True

    Returns:
        None

    """
    # Initialize key variables
    threads_in_pool = 10

    # Create directory if needed
    perm_dir = config.snmp_directory()
    temp_dir = tempfile.mkdtemp()

    # Delete all files in temporary directory
    jm_general.delete_files(temp_dir)

    # Spawn a pool of threads, and pass them queue instance
    for _ in range(threads_in_pool):
        update_thread = PollOids(THREAD_QUEUE)
        update_thread.daemon = True
        update_thread.start()

    # Get host data and write to file
    for host in config.hosts():
        ####################################################################
        #
        # Define variables that will be required for the database update
        # We have to initialize the dict during every loop to prevent
        # data corruption
        #
        ####################################################################
        data_dict = {}
        data_dict['host'] = host
        data_dict['config'] = config
        data_dict['verbose'] = verbose
        data_dict['temp_dir'] = temp_dir
        THREAD_QUEUE.put(data_dict)

    # Wait on the queue until everything has been processed
    THREAD_QUEUE.join()

    # PYTHON BUG. Join can occur while threads are still shutting down.
    # This can create spurious "Exception in thread (most likely raised
    # during interpreter shutdown)" errors.
    # The "time.sleep(1)" adds a delay to make sure things really terminate
    # properly. This seems to be an issue on virtual machines in Dev only
    time.sleep(1)

    # Cleanup, move temporary files to clean permanent directory.
    # Delete temporary directory
    if os.path.isdir(perm_dir):
        jm_general.delete_files(perm_dir)
    else:
        os.makedirs(perm_dir, 0o755)
    jm_general.move_files(temp_dir, perm_dir)
    os.rmdir(temp_dir)
