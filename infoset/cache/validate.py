#!/usr/bin/env python3

"""Demonstration Script that extracts agent data from cache directory files.

This could be a modified to be a daemon

"""

# Standard libraries
import os
import re
import json

# Infoset libraries
from infoset.utils import log
from infoset.utils import jm_general


class ValidateCache(object):
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

    def __init__(self, filepath=None, data=None):
        """Method initializing the class.

        Args:
            filepath: Cache filename
            data: Data dict expected to be in a cache file (Agent or server)

        Returns:
            None

        """
        # Initialize key variables
        self.validated = True
        self.information = {}
        self.filename = None
        self.filepath = filepath

        # Filenames must start with a numeric timestamp and #
        # end with a hex string. This will be tested later
        regex = re.compile(r'^\d+_[0-9a-f]+.json')

        if filepath is not None:
            # Try reading file if filename format is OK
            self.filename = os.path.basename(filepath)
            if bool(regex.match(self.filename)) is True:
                # Ingest data
                try:
                    with open(filepath, 'r') as f_handle:
                        self.information = json.load(f_handle)
                except:
                    self.information = {}
                    self.validated = False
            else:
                self.validated = False
        else:
            if isinstance(data, dict) is True:
                self.information = data
            else:
                self.validated = False

    def getinfo(self):
        """Provide validated information when valid.

        Args:
            None

        Returns:
            data: Data

        """
        # Initialize key variables
        data = False

        # Return
        if self.valid() is True:
            data = self.information
        return data

    def valid(self):
        """Master method that defines whether data is OK.

        Args:
            None

        Returns:
            all_ok:

        """
        # Initialize key variables
        validity = [self.validated]

        # Append results of tests
        validity.append(self.check_meta())
        validity.append(self.check_data_types())

        # Do final check
        if False in validity:
            all_ok = False
            # Error message
            if self.filepath is not None:
                log_message = (
                    'Cache file %s is invalid'
                    '') % (self.filepath)
                log.log2warn(1021, log_message)
            else:
                log_message = ('Cache data is invalid')
                log.log2warn(1059, log_message)
        else:
            all_ok = True
        return all_ok

    def check_meta(self):
        """Method initializing the class.

        Args:
            None

        Returns:
            valid: True if valid

        """
        # Initialize key variables
        valid = True
        agent_meta_keys = ['timestamp', 'uid', 'agent', 'hostname']

        # Verify universal parameters from file
        for key in agent_meta_keys:
            if key not in self.information:
                valid = False

        # Get agent name for future reporting
        if valid is True:
            # Timestamp must be an integer
            try:
                int(self.information['timestamp'])
            except:
                valid = False

            # Parse filename for information
            if self.filename is not None:
                (name, _) = self.filename.split('.')
                (tstamp, uid) = name.split('_')
                timestamp = int(tstamp)

                # Double check that the UID and timestamp in the
                # filename matches that in the file.
                # Ignore invalid files as a safety measure.
                # Don't try to delete. They could be owned by some
                # one else and the daemon could crash
                if uid != self.information['uid']:
                    valid = False
                if timestamp != self.information['timestamp']:
                    valid = False
                if jm_general.validate_timestamp(timestamp) is False:
                    valid = False

        # Return
        return valid

    def check_data_types(self):
        """Method initializing the class.

        Args:
            None

        Returns:
            valid: True if valid

        """
        # Initialize key variables
        valid = True
        data_types = ['chartable', 'other']

        # Process chartable data
        for data_type in data_types:
            # Skip if data type isn't in the data
            if data_type not in self.information:
                continue

            # Process the data type
            for category, group in sorted(
                    self.information[data_type].items()):
                # Process keys
                for key in ['base_type', 'description', 'data']:
                    if key not in group:
                        valid = False

                # Process data
                for datapoint in group['data']:
                    if len(datapoint) != 3:
                        valid = False

                    # Check to make sure value is numeric
                    if category == 'chartable':
                        value = datapoint[1]
                        try:
                            float(value)
                        except:
                            valid = False
        # Return
        return valid
