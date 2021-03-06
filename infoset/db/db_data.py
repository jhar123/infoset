"""Module of infoset database functions.

Classes for agent data

"""
# Python standard libraries
from collections import defaultdict
from pprint import pprint

# Infoset libraries
from infoset.utils import log
from infoset.utils import jm_general
from infoset.db import db_datapoint
from infoset.db import db


class GetIDX(object):
    """Class to return agent data.

    Args:
        None

    Returns:
        None

    Methods:

    """

    def __init__(self, idx, config, start=None, stop=None):
        """Function for intializing the class.

        Args:
            idx: idx of datapoint
            config: Config object
            start: Starting timestamp
            stop: Ending timestamp

        Returns:
            None

        """
        # Initialize important variables
        self.data = defaultdict(dict)

        # Get the datapoint's base_type
        datapointer = db_datapoint.GetIDX(idx, config)
        self.base_type = datapointer.base_type()

        # Redefine start / stop times
        if start is None:
            self.ts_start = jm_general.normalized_timestamp() - (3600 * 24)
        else:
            # Adjust for counters
            if self.base_type == 1:
                self.ts_start = start
            else:
                self.ts_start = start - 300
        if stop is None:
            self.ts_stop = jm_general.normalized_timestamp()
        else:
            self.ts_stop = stop
        if self.ts_start > self.ts_stop:
            self.ts_start = self.ts_stop

        # Prepare SQL query to read a record from the database.
        # Only active oids
        sql_query = (
            'SELECT value, timestamp '
            'FROM iset_data '
            'WHERE '
            '(timestamp >= %s AND timestamp <= %s) AND '
            'idx_datapoint=\'%s\'') % (
                self.ts_start, self.ts_stop, idx)

        # Do query and get results
        database = db.Database(config)
        query_results = database.query(sql_query, 1301)

        # Massage data
        for row in query_results:
            # uid found?
            if not row[0]:
                log_message = ('idx %s not found.') % (idx)
                log.log2die(1302, log_message)

            # Assign values
            timestamp = row[1]
            value = row[0]
            self.data[timestamp] = value

    def everything(self):
        """Get all datapoints.

        Args:
            None

        Returns:
            value: Dictionary of data_points

        """
        # Return data
        value = self._counter()
        return value

    def _counter(self):
        """Convert counter data to gauge.

        Args:
            None

        Returns:
            values: Converted dict of data keyed by timestamp

        """
        # Initialize key variables
        count = 0

        # Populate values dictionary with zeros. This ensures that
        # all timestamp values are covered if we have lost contact
        # with the agent at some point along the time series.
        if self.base_type == 1:
            values = dict.fromkeys(
                range(self.ts_start, self.ts_stop + 300, 300), 0)
        else:
            values = dict.fromkeys(
                range(self.ts_start + 300, self.ts_stop + 300, 300), 0)

        # Start conversion
        for timestamp, value in sorted(self.data.items()):
            # Process counter values
            if self.base_type != 1:
                # Skip first value
                if count == 0:
                    old_timestamp = timestamp
                    count += 1
                    continue

                # Get new value
                new_value = value - self.data[old_timestamp]

                # Do conversion
                if new_value >= 0:
                    values[timestamp] = new_value
                else:
                    if self.base_type == 32:
                        fixed_value = 4294967296 + abs(value) - 1
                    else:
                        fixed_value = (4294967296 * 4294967296) + abs(value) - 1
                    values[timestamp] = fixed_value
            else:
                # Process gauge values
                values[timestamp] = self.data[timestamp]

            # Save old timestamp
            old_timestamp = timestamp

        # Return
        return values
