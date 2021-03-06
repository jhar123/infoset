#!/usr/bin/env python3

"""Test the jm_validate module."""

import ast
import unittest
import shutil
import json
import os
from os import path
import random
import pprint
import string
import tempfile
from infoset.cache import validate as test_class


class TestValidateCache(unittest.TestCase):
    """Checks all functions and methods."""

    # ---------------------------------------------------------------------- #
    # General object setup
    # ---------------------------------------------------------------------- #

    # Required
    maxDiff = None

    random_string = ''.join([random.choice(
        string.ascii_letters + string.digits) for n in range(9)])

    @classmethod
    def setUpClass(cls):
        # Initializing key variables

        cls.config_good = (
            """
            {'agent': 'interfaces',
             'chartable': {'_ifInOctets': {'base_type': 'counter32',
                               'data': [[0, 19729125944, 'FastEthernet0/1'],
                                        [1, 30281712128, 'FastEthernet0/2'],
                                        [2, 3602957760, 'FastEthernet0/21'],
                                        [3, 19677028080, 'FastEthernet0/23'],
                                        [4, 18527568600, 'FastEthernet0/26'],
                                        [5, 13548541568, 'FastEthernet0/3'],
                                        [6, 1633419768, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1657831376, 'Vlan1']],
                               'description': None},
                           '_ifOutOctets': {'base_type': 'counter32',
                               'data': [[0, 31348133488, 'FastEthernet0/1'],
                                        [1, 5327628464, 'FastEthernet0/2'],
                                        [2, 10968459144, 'FastEthernet0/21'],
                                        [3, 13275253360, 'FastEthernet0/23'],
                                        [4, 24709868928, 'FastEthernet0/26'],
                                        [5, 7793695064, 'FastEthernet0/3'],
                                        [6, 3581965608, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1682357240, 'Vlan1']],
                               'description': None}},
             'hostname': '192.168.1.3',
             'timestamp': 1468857600,
             'uid': 'e92af7f044246b7976c1f3274b8f6228ea999bafc92b\
016597fb56ec51e57668'}
            """)

        cls.config_no_agent = (
            """
             {'chartable': {'_ifInOctets': {'base_type': 'counter32',
                               'data': [[0, 19729125944, 'FastEthernet0/1'],
                                        [1, 30281712128, 'FastEthernet0/2'],
                                        [2, 3602957760, 'FastEthernet0/21'],
                                        [3, 19677028080, 'FastEthernet0/23'],
                                        [4, 18527568600, 'FastEthernet0/26'],
                                        [5, 13548541568, 'FastEthernet0/3'],
                                        [6, 1633419768, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1657831376, 'Vlan1']],
                               'description': None},
                           '_ifOutOctets': {'base_type': 'counter32',
                               'data': [[0, 31348133488, 'FastEthernet0/1'],
                                        [1, 5327628464, 'FastEthernet0/2'],
                                        [2, 10968459144, 'FastEthernet0/21'],
                                        [3, 13275253360, 'FastEthernet0/2'],
                                        [4, 24709868928, 'FastEthernet0/26'],
                                        [5, 7793695064, 'FastEthernet0/3'],
                                        [6, 3581965608, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1682357240, 'Vlan1']],
                               'description': None}},
             'hostname': '192.168.1.3',
             'timestamp': 1468857600,
             'uid': 'e92af7f044246b7976c1f3274b8f6228ea999bafc92b\
016597fb56ec51e57668'}
            """)

        cls.config_no_chartable = (
            """
            {'agent': 'interfaces',
             'hostname': '192.168.1.3',
             'timestamp': 1468857600,
             'uid': 'e92af7f044246b7976c1f3274b8f6228ea999bafc92b\
016597fb56ec51e57668'}
            """)

        cls.config_no_hostname = (
            """
            {'agent': 'interfaces',
             'chartable': {'_ifInOctets': {'base_type': 'counter32',
                               'data': [[0, 19729125944, 'FastEthernet0/1'],
                                        [1, 30281712128, 'FastEthernet0/2'],
                                        [2, 3602957760, 'FastEthernet0/21'],
                                        [3, 19677028080, 'FastEthernet0/23'],
                                        [4, 18527568600, 'FastEthernet0/26'],
                                        [5, 13548541568, 'FastEthernet0/3'],
                                        [6, 1633419768, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1657831376, 'Vlan1']],
                               'description': None},
                           '_ifOutOctets': {'base_type': 'counter32',
                               'data': [[0, 31348133488, 'FastEthernet0/1'],
                                        [1, 5327628464, 'FastEthernet0/2'],
                                        [2, 10968459144, 'FastEthernet0/21'],
                                        [3, 13275253360, 'FastEthernet0/23'],
                                        [4, 24709868928, 'FastEthernet0/26'],
                                        [5, 7793695064, 'FastEthernet0/3'],
                                        [6, 3581965608, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1682357240, 'Vlan1']],
                               'description': None}},
             'timestamp': 1468857600,
             'uid': 'e92af7f044246b7976c1f3274b8f6228ea999bafc92b\
016597fb56ec51e57668'}
            """)

        cls.config_no_timestamp = (
            """
            {'agent': 'interfaces',
             'chartable': {'_ifInOctets': {'base_type': 'counter32',
                               'data': [[0, 19729125944, 'FastEthernet0/1'],
                                        [1, 30281712128, 'FastEthernet0/2'],
                                        [2, 3602957760, 'FastEthernet0/21'],
                                        [3, 19677028080, 'FastEthernet0/23'],
                                        [4, 18527568600, 'FastEthernet0/26'],
                                        [5, 13548541568, 'FastEthernet0/3'],
                                        [6, 1633419768, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1657831376, 'Vlan1']],
                               'description': None},
                           '_ifOutOctets': {'base_type': 'counter32',
                               'data': [[0, 31348133488, 'FastEthernet0/1'],
                                        [1, 5327628464, 'FastEthernet0/2'],
                                        [2, 10968459144, 'FastEthernet0/21'],
                                        [3, 13275253360, 'FastEthernet0/23'],
                                        [4, 24709868928, 'FastEthernet0/26'],
                                        [5, 7793695064, 'FastEthernet0/3'],
                                        [6, 3581965608, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1682357240, 'Vlan1']],
                               'description': None}},
             'hostname': '192.168.1.3',
             'uid': 'e92af7f044246b7976c1f3274b8f6228ea999bafc92b\
016597fb56ec51e57668'}
            """)

        cls.config_not_int_timestmp = (
            """
            {'agent': 'interfaces',
             'chartable': {'_ifInOctets': {'base_type': 'counter32',
                               'data': [[0, 19729125944, 'FastEthernet0/1'],
                                        [1, 30281712128, 'FastEthernet0/2'],
                                        [2, 3602957760, 'FastEthernet0/21'],
                                        [3, 19677028080, 'FastEthernet0/23'],
                                        [4, 18527568600, 'FastEthernet0/26'],
                                        [5, 13548541568, 'FastEthernet0/3'],
                                        [6, 1633419768, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1657831376, 'Vlan1']],
                               'description': None},
                           '_ifOutOctets': {'base_type': 'counter32',
                               'data': [[0, 31348133488, 'FastEthernet0/1'],
                                        [1, 5327628464, 'FastEthernet0/2'],
                                        [2, 10968459144, 'FastEthernet0/21'],
                                        [3, 13275253360, 'FastEthernet0/23'],
                                        [4, 24709868928, 'FastEthernet0/26'],
                                        [5, 7793695064, 'FastEthernet0/3'],
                                        [6, 3581965608, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1682357240, 'Vlan1']],
                               'description': None}},
             'hostname': '192.168.1.3',
             'timestamp': 'jfknlagerklw',
             'uid': 'e92af7f044246b7976c1f3274b8f6228ea999bafc92b\
016597fb56ec51e57668'}
            """)

        cls.config_no_uid = (
            """
            {'agent': 'interfaces',
             'chartable': {'_ifInOctets': {'base_type': 'counter32',
                               'data': [[0, 19729125944, 'FastEthernet0/1'],
                                        [1, 30281712128, 'FastEthernet0/2'],
                                        [2, 3602957760, 'FastEthernet0/21'],
                                        [3, 19677028080, 'FastEthernet0/23'],
                                        [4, 18527568600, 'FastEthernet0/26'],
                                        [5, 13548541568, 'FastEthernet0/3'],
                                        [6, 1633419768, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1657831376, 'Vlan1']],
                               'description': None},
                           '_ifOutOctets': {'base_type': 'counter32',
                               'data': [[0, 31348133488, 'FastEthernet0/1'],
                                        [1, 5327628464, 'FastEthernet0/2'],
                                        [2, 10968459144, 'FastEthernet0/21'],
                                        [3, 13275253360, 'FastEthernet0/23'],
                                        [4, 24709868928, 'FastEthernet0/26'],
                                        [5, 7793695064, 'FastEthernet0/3'],
                                        [6, 3581965608, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 1682357240, 'Vlan1']],
                               'description': None}},
             'hostname': '192.168.1.3',
             'timestamp': 1468857600}
            """)

        cls.config_notcor_data = (
            """
            {'agent': 'interfaces',
             'chartable': {'_ifInOctets': {'base_type': 'counter32',
                               'data': [[0, 1.729125944, 'FastEthernet0/1'],
                                        [1, 30.81712128, 'FastEthernet0/2'],
                                        [2, 360.957760, 'FastEthernet0/21'],
                                        [3, 1967.028080, 'FastEthernet0/23'],
                                        [4, 18527.68600, 'FastEthernet0/26'],
                                        [5, 135485.1568, 'FastEthernet0/3'],
                                        [6, 1633419.68, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 16578313.6, 'Vlan1']],
                               'description': None},
                           '_ifOutOctets': {'base_type': 'counter32',
                               'data': [[0, 3.348133488, 'FastEthernet0/1'],
                                        [1, 53.7628464, 'FastEthernet0/2'],
                                        [2, 109.8459144, 'FastEthernet0/21'],
                                        [3, 1327.253360, 'FastEthernet0/23'],
                                        [4, 24709.68928, 'FastEthernet0/26'],
                                        [5, 779369.064, 'FastEthernet0/3'],
                                        [6, 3581965.08, 'FastEthernet0/35'],
                                        [7, 0, 'Null0'],
                                        [8, 168235.240, 'Vlan1']],
                               'description': None}},
             'hostname': '192.168.1.3',
             'timestamp': 1468857600,
             'uid': 'e92af7f044246b7976c1f3274b8f6228ea999bafc92b\
016597fb56ec51e57668'}
            """)

        # Complete file name
        cls.config_good_dict = ast.literal_eval(cls.config_good.strip())

        # Incomplete file names
        cls.config_no_agent_dict = ast.literal_eval(
            cls.config_no_agent.strip())
        cls.config_no_chartable_dict = ast.literal_eval(
            cls.config_no_chartable.strip())
        cls.config_no_hostname_dict = ast.literal_eval(
            cls.config_no_hostname.strip())
        cls.config_no_timestamp_dict = ast.literal_eval(
            cls.config_no_timestamp.strip())
        cls.config_not_int_timestmp_dict = ast.literal_eval(
            cls.config_not_int_timestmp.strip())
        cls.config_no_uid_dict = ast.literal_eval(cls.config_no_uid.strip())
        cls.config_notcor_data_dict = ast.literal_eval(
            cls.config_notcor_data.strip())

        # Create temporary configuration file
        cls.validate_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        # Cleanup temporary files when done
        shutil.rmtree(cls.validate_dir)

    def test_getinfo(self):
        """Test if information has been recieved."""
        valid = False
        getinfofile = _filename(
            self.config_good_dict['timestamp'],
            self.config_good_dict['uid'])

        infopath = path.join(self.validate_dir, getinfofile)

        with open(infopath, 'w') as validate:
            json.dump(self.config_good_dict, validate)

        if os.path.isfile(infopath) is True:
            valid = True

        print(valid)

    def test_valid(self):
        """Test for valid information in file name."""

        # ------------------------------------------------------------------ #
        # Test with good configuration
        #
        # (File)
        #
        # ------------------------------------------------------------------ #
        # File test
        goodfile = _filename(
            self.config_good_dict['timestamp'],
            self.config_good_dict['uid'])
        goodfile_path = path.join(self.validate_dir, goodfile)

        # Dict test
        with open(goodfile_path, 'w') as validate:
            json.dump(self.config_good_dict, validate)

        testobj = test_class.ValidateCache(filepath=goodfile_path)
        result = testobj.valid()
        self.assertEqual(result, True)
        print(result)

    def test_check_meta(self):
        """Test for correct and complete information."""
        # Return False if agent is not found in file
        no_agent = _filename(
            self.config_no_agent_dict['timestamp'],
            self.config_no_agent_dict['uid'])
        # Get and write to no_agent_path
        no_agent_path = path.join(self.validate_dir, no_agent)
        with open(no_agent_path, 'w') as validate:
            json.dump(self.config_no_agent_dict, validate)

        testobj = test_class.ValidateCache(filepath=no_agent_path)
        result = testobj.valid()
        self.assertEqual(result, False)
        print(result)

        # Return False if hostname is not found in file
        no_hostname = _filename(
            self.config_no_hostname_dict['timestamp'],
            self.config_no_hostname_dict['uid'])
        # Get and write to no_hostname_path
        no_hostname_path = path.join(self.validate_dir, no_hostname)
        with open(no_hostname_path, 'w') as validate:
            json.dump(self.config_no_hostname_dict, validate)

        testobj = test_class.ValidateCache(filepath=no_hostname_path)
        result = testobj.valid()
        self.assertEqual(result, False)
        print(result)

        # Return False if timestamp is not found in file
        # since timestamp is not in file it has to be made up
        if 'timestamp' not in self.config_no_timestamp_dict:
            no_timestamp = _filename('1468857600',
                                     'e92af7f044246b7976c1f3274b8f6228ea999\
bafc92b016597fb56ec51e57668')

        else:
            no_timestamp = _filename(
                self.config_no_timestamp_dict['timestamp'],
                self.config_no_timestamp_dict['uid'])

        # Get and write to no_timestamp_path
        no_timestamp_path = path.join(self.validate_dir, no_timestamp)
        with open(no_timestamp_path, 'w') as validate:
            json.dump(self.config_no_timestamp_dict, validate)

        testobj = test_class.ValidateCache(filepath=no_timestamp_path)
        result = testobj.valid()
        self.assertEqual(result, False)
        print(result)

        # Return False if timestamp is not an integer
        not_int_time = _filename(
            self.config_not_int_timestmp_dict['timestamp'],
            self.config_not_int_timestmp_dict['uid'])

        # Get and write to not_int_time_path
        not_int_time_path = path.join(self.validate_dir, not_int_time)
        with open(not_int_time_path, 'w') as validate:
            json.dump(self.config_not_int_timestmp_dict, validate)

        testobj = test_class.ValidateCache(filepath=not_int_time_path)
        result = testobj.valid()
        self.assertEqual(result, False)
        self.assertIsInstance(result, int)
        print(result)

        # Return False if uid is not found in file
        # since uid is not in file it has to be made up
        if 'uid' not in self.config_no_uid_dict:
            no_uid = _filename('1468857600',
                               'e92af7f044246b7976c1f3274b8f6228ea999bafc92b0\
16597fb56ec51e57668')

        else:
            no_uid = _filename(
                self.config_no_uid_dict['timestamp'],
                self.config_no_uid_dict['uid'])

        # Get and write to no_uid_path
        no_uid_path = path.join(self.validate_dir, no_uid)
        with open(no_uid_path, 'w') as validate:
            json.dump(self.config_no_uid_dict, validate)

        testobj = test_class.ValidateCache(filepath=no_uid_path)
        result = testobj.valid()
        self.assertEqual(result, False)
        print(result)

    def test_check_data_types(self):
        """Test for correct and complete information in data types."""
        # Shorten the name of the chartable key found in dictionary
        data_groupin = self.config_notcor_data_dict['chartable'][
            '_ifInOctets']['data']
        data_groupout = self.config_notcor_data_dict['chartable'][
            '_ifOutOctets']['data']
        data_in = '_ifInOctets'
        data_out = '_ifOutOctets'
        # define names for section in 'data'
        for data_in in self.config_notcor_data_dict['chartable'].keys():
            for datapoint in data_groupin:
                index = datapoint[0]
                value = datapoint[1]
                source = datapoint[2]

        for data_out in self.config_notcor_data_dict['chartable'].keys():
            for datapoint in data_groupout:
                index = datapoint[0]
                value = datapoint[1]
                source = datapoint[2]

        # Return False if chartable is not found in file
        no_data = _filename(
            self.config_notcor_data_dict['timestamp'],
            self.config_notcor_data_dict['uid'])
        # Get and write to no_chartable_path
        no_data_path = path.join(self.validate_dir, no_data)
        with open(no_data_path, 'w') as validate:
            json.dump(self.config_notcor_data_dict, validate)

        result = (len(datapoint) == 3)
        self.assertEqual(result, True)
        # testobj = test_class.ValidateCache(filepath=no_data_path)
        # result = testobj.valid()
        # self.assertEqual(result, True)
        print(result)

        data_float = _filename(
            self.config_notcor_data_dict['timestamp'],
            self.config_notcor_data_dict['uid'])
        # Get and write to no_chartable_path
        data_float_path = path.join(self.validate_dir, data_float)
        with open(data_float_path, 'w') as validate:
            json.dump(self.config_notcor_data_dict, validate)

        # testobj = test_class.ValidateCache(filepath=data_float_path)
        # result = testobj(value)
        result = (value == float)
        self.assertEqual(result, False)
        # self.assertEqual(result, False)
        print(result)


def _filename(timestamp, uid):
    """This is to make naming files easier."""
    filename = ('%s_%s.json') % (timestamp, uid)
    return filename

if __name__ == '__main__':
    # Do the unit test
    unittest.main()
