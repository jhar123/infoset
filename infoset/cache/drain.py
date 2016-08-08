#!/usr/bin/env python3

"""Demonstration Script that extracts agent data from cache directory files.

This could be a modified to be a daemon

"""

# Standard libraries
import os
import hashlib
from collections import defaultdict

# Infoset libraries
from infoset.utils import log
from infoset.cache import validate


class Drain(object):
    """Infoset class that ingests agent data.

    Args:
        None

    Returns:
        None

    Methods:
        __init__:
        populate:
        post:
    """

    def __init__(self, filename):
        """Method initializing the class.

        Args:
            filename: Cache filename

        Returns:
            None

        """
        # Initialize key variables
        self.filename = filename
        self.data = defaultdict(lambda: defaultdict(dict))
        self.metadata = []
        self.validated = False
        self.agent_meta = {}
        data_types = ['chartable', 'other']
        agent_meta_keys = ['timestamp', 'uid', 'agent', 'hostname']

        # Ingest data
        validator = validate.ValidateCache(filename)
        information = validator.getinfo()

        # Log if data is bad
        if information is False:
            log_message = (
                'Cache ingest file %s is invalid.') % (filename)
            log.log2warn(1051, log_message)
            return
        else:
            self.validated = True

        if self.validated is True:
            # Get universal parameters from file
            for key in agent_meta_keys:
                self.agent_meta[key] = information[key]
            timestamp = int(information['timestamp'])
            uid = information['uid']

            # Process chartable data
            for data_type in data_types:
                # Skip if data type isn't in the data
                if data_type not in information:
                    continue

                # Process the data type
                for label, group in sorted(information[data_type].items()):
                    # Get universal parameters for group
                    base_type = _base_type(group['base_type'])
                    description = group['description']

                    # Initialize base type
                    if base_type not in self.data[data_type]:
                        self.data[data_type][base_type] = []

                    # Process data
                    for datapoint in group['data']:
                        index = datapoint[0]
                        value = datapoint[1]
                        source = datapoint[2]
                        did = _did(uid, label, index)

                        # Update data
                        self.data[data_type][base_type].append(
                            (uid, did, value, timestamp)
                        )

                        # Update sources
                        self.metadata.append(
                            (uid, did, label, source, description, base_type)
                        )

    def valid(self):
        """Determine whether data is valid.

        Args:
            None

        Returns:
            isvalid: Valid if true

        """
        # Initialize key variables
        isvalid = self.validated

        # Return
        return isvalid

    def uid(self):
        """Return uid.

        Args:
            None

        Returns:
            data: Agent UID

        """
        # Initialize key variables
        data = self.agent_meta['uid']

        # Return
        return data

    def timestamp(self):
        """Return timestamp.

        Args:
            None

        Returns:
            data: Agent timestamp

        """
        # Initialize key variables
        data = int(self.agent_meta['timestamp'])

        # Return
        return data

    def agent(self):
        """Return agent.

        Args:
            None

        Returns:
            data: Agent agent_name

        """
        # Initialize key variables
        data = self.agent_meta['agent']

        # Return
        return data

    def hostname(self):
        """Return hostname.

        Args:
            None

        Returns:
            data: Agent hostname

        """
        # Initialize key variables
        data = self.agent_meta['hostname']

        # Return
        return data

    def counter32(self):
        """Return counter32 chartable data from file.

        Args:
            None

        Returns:
            data: List of tuples (uid, did, value, timestamp)
                uid = UID of device providing data
                did = Datapoint ID
                value = Value of datapoint
                timestamp = Timestamp when data was collected by the agent

        """
        # Initialize key variables
        data = []

        # Get data
        if 'chartable' in self.data:
            if 32 in self.data['chartable']:
                data = self.data['chartable'][32]

        # Return
        return data

    def counter64(self):
        """Return counter64 chartable data from file.

        Args:
            None

        Returns:
            data: List of tuples (uid, did, value, timestamp)
                uid = UID of device providing data
                did = Datapoint ID
                value = Value of datapoint
                timestamp = Timestamp when data was collected by the agent

        """
        # Initialize key variables
        data = []

        # Get data
        if 'chartable' in self.data:
            if 64 in self.data['chartable']:
                data = self.data['chartable'][64]

        # Return
        return data

    def floating(self):
        """Return floating chartable data from file.

        Args:
            None

        Returns:
            data: List of tuples (uid, did, value, timestamp)
                uid = UID of device providing data
                did = Datapoint ID
                value = Value of datapoint
                timestamp = Timestamp when data was collected by the agent

        """
        # Initialize key variables
        data = []

        # Get data
        if 'chartable' in self.data:
            if 1 in self.data['chartable']:
                data = self.data['chartable'][1]

        # Return
        return data

    def chartable(self):
        """Return all chartable data from file.

        Args:
            None

        Returns:
            data: List of tuples (uid, did, value, timestamp)
                uid = UID of device providing data
                did = Datapoint ID
                value = Value of datapoint
                timestamp = Timestamp when data was collected by the agent

        """
        # Initialize key variables
        data = []

        # Initialize key variables
        data.extend(self.floating())
        data.extend(self.counter32())
        data.extend(self.counter64())

        # Return
        return data

    def other(self):
        """Return other non-chartable data from file.

        Args:
            None

        Returns:
            data: List of tuples (uid, did, value, timestamp)
                uid = UID of device providing data
                did = Datapoint ID
                value = Value of datapoint
                timestamp = Timestamp when data was collected by the agent

        """
        # Initialize key variables
        data = []

        # Return (Ignore whether floating or counter)
        if 'other' in self.data:
            for _, value in self.data['other'].items():
                data.extend(value)
        return data

    def sources(self):
        """Return sources data from file.

        Args:
            None

        Returns:
            data: List of tuples (uid, did, label, source, description)
                uid = UID of device providing data
                did = Datapoint ID
                label = Label that the agent gave the category of datapoint
                source = Subsystem that provided the data in the datapoint
                description = Description of the label
                base_type = SNMP base type code (Counter32, Gauge etc.)

        """
        # Initialize key variables
        data = self.metadata

        # Return
        return data

    def purge(self):
        """Purge cache file that was read.

        Args:
            None

        Returns:
            success: "True" if successful

        """
        # Initialize key variables
        success = True

        try:
            os.remove(self.filename)
        except:
            success = False

        # Report success
        if success is True:
            log_message = (
                'Ingest cache file %s deleted') % (self.filename)
            log.log2quiet(1046, log_message)
        else:
            log_message = (
                'Failed to delete ingest cache file %s') % (self.filename)
            log.log2warn(1050, log_message)

        # Return
        return success


def _did(uid, label, index):
    """Create a unique DID from ingested data.

    Args:
        uid: UID of device that created the cache data file
        label: Label of the data
        index: Index of the data

    Returns:
        did: Datapoint ID

    """
    # Initialize key variables
    prehash = ('%s%s%s') % (uid, label, index)
    hasher = hashlib.sha256()
    hasher.update(bytes(prehash.encode()))
    did = hasher.hexdigest()

    # Return
    return did


def _base_type(data):
    """Create a base_type integer value from the string sent by agents.

    Args:
        data: base_type value as string

    Returns:
        base_type: Base type value as integer

    """
    # Initialize key variables
    if bool(data) is False:
        value = 'NULL'
    else:
        value = data

    # Assign base type code
    if value.lower() == 'floating':
        base_type = 1
    elif value.lower() == 'counter32':
        base_type = 32
    elif value.lower() == 'counter64':
        base_type = 64
    else:
        base_type = 0

    # Return
    return base_type
