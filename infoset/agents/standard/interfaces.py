#!/usr/bin/env python3
"""infoset Sentry3 (Servertech intelligent CDU power strip) agent.

Description:

    This script:
        1) Retrieves a variety of system information
        2) Posts the data using HTTP to a server listed
           in the configuration file

"""
# Standard libraries
import logging
from collections import defaultdict

# infoset libraries
from infoset.agents import agent as Agent
from infoset.utils import log
from infoset.utils import jm_configuration
from infoset.snmp import snmp_manager
from infoset.snmp import mib_if
from infoset.snmp import mib_if_64

logging.getLogger('requests').setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG)


class PollingAgent(object):
    """Infoset agent that gathers data.

    Args:
        None

    Returns:
        None

    Functions:
        __init__:
        populate:
        post:
    """

    def __init__(self, config_dir):
        """Method initializing the class.

        Args:
            config_dir: Configuration directory

        Returns:
            None

        """
        # Initialize key variables
        self.agent_name = 'interfaces'

        # Get configuration
        self.config = jm_configuration.ConfigAgent(
            config_dir, self.agent_name)

        # Get snmp configuration information from infoset
        self.snmp_config = jm_configuration.ConfigSNMP(config_dir)

    def name(self):
        """Return agent name.

        Args:
            None

        Returns:
            value: Name of agent

        """
        # Return
        value = self.agent_name
        return value

    def query(self):
        """Query all remote hosts for data.

        Args:
            None

        Returns:
            None

        """
        # Check each hostname
        hostnames = self.config.agent_snmp_hostnames()
        for hostname in hostnames:
            # Get valid SNMP credentials
            validate = snmp_manager.Validate(
                hostname, self.snmp_config.snmp_auth())
            snmp_params = validate.credentials()

            # Log message
            if snmp_params is None:
                log_message = (
                    'No valid SNMP configuration found '
                    'for host "%s" ') % (hostname)
                log.log2warn(1022, log_message)
                continue

            # Create Query make sure MIB is supported
            snmp_object = snmp_manager.Interact(snmp_params)
            query = mib_if.init_query(snmp_object)
            query64 = mib_if_64.init_query(snmp_object)
            if query.supported() is False:
                log_message = (
                    'The IF-MIB is not supported by host  "%s"'
                    '') % (hostname)
                log.log2warn(1024, log_message)
                continue

            # Get the UID for the agent after all preliminary checks are OK
            uid_env = Agent.get_uid(hostname)

            # Post data to the remote server
            self.upload(uid_env, hostname, query, query64)

    def upload(self, uid, hostname, query, query64):
        """Post system data to the central server.

        Args:
            uid: Unique ID for Agent
            hostname: Hostname
            query: SNMP credentials object (IF-MIB 32 bit)
            query64: SNMP credentials object (IF-MIB 64 bit)

        Returns:
            None

        """
        # Initialize key variables
        ignore = []
        agent = Agent.Agent(uid, self.config, hostname)

        # Get a list of interfaces to ignore because they are down
        status = query.ifoperstatus()
        for key, value in status.items():
            if value != 1:
                ignore.append(key)

        # Get descriptions
        descriptions = query.ifdescr(safe=True)
        if bool(descriptions) is False:
            return

        # Update 32 bit data
        _update_32(agent, ignore, descriptions, query)

        # Update 64 bit data
        _update_64(agent, ignore, descriptions, query64)

        # Post data
        success = agent.post()

        # Purge cache if success is True
        if success is True:
            agent.purge()


def _update_64(agent, ignore, descriptions, query64):
    """Return all the CLI options.

    Args:
        agent: Agent object
        ignore: List of ifIndexes to ignore
        descriptions: Dict keyed by ifIndex of ifDescr values
        query64: SNMP query object (64 bit)

    Returns:
        None

    """
    # Initialize key variables
    prefix = ''
    state = {}
    data = defaultdict(lambda: defaultdict(dict))

    # Don't provide 64 bit counter data if not supported
    if query64.supported() is False:
        return

    ##########################################################################
    # Handle 64 bit counters
    ##########################################################################
    labels = ['ifHCInOctets', 'ifHCOutOctets']
    state['ifHCInOctets'] = query64.ifhcinoctets(safe=True)
    state['ifHCOutOctets'] = query64.ifhcoutoctets(safe=True)

    # Make sure we received values
    for label in labels:
        if bool(state[label]) is False:
            return

    # Create dictionary for eventual posting
    for label in labels:
        for key, value in state[label].items():
            if key in ignore:
                continue
            source = descriptions[key]
            data[label][source] = value * 8

    # Populate agent
    agent.populate_dict(prefix, data, base_type='counter64')


def _update_32(agent, ignore, descriptions, query):
    """Return all the CLI options.

    Args:
        agent: Agent object
        ignore: List of ifIndexes to ignore
        descriptions: Dict keyed by ifIndex of ifDescr values
        query: SNMP query object

    Returns:
        None

    """
    # Initialize key variables
    prefix = ''
    state = {}
    data = defaultdict(lambda: defaultdict(dict))

    ##########################################################################
    # Handle 32 bit counters
    ##########################################################################
    # Get results from querying device
    labels = ['ifInOctets', 'ifOutOctets']
    state['ifInOctets'] = query.ifinoctets(safe=True)
    state['ifOutOctets'] = query.ifoutoctets(safe=True)

    # Make sure we received values
    for label in labels:
        if bool(state[label]) is False:
            return

    # Create dictionary for eventual posting
    for label in labels:
        for key, value in state[label].items():
            if key in ignore:
                continue
            source = descriptions[key]
            data[label][source] = value * 8

    # Populate agent
    agent.populate_dict(prefix, data, base_type='counter32')


def main():
    """Start the infoset agent.

    Args:
        None

    Returns:
        None

    """
    # Get configuration
    cli = Agent.AgentCLI()
    config_dir = cli.config_dir()
    poller = PollingAgent(config_dir)

    # Do control
    cli.control(poller)

if __name__ == "__main__":
    main()
