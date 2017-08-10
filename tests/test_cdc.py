''' tests.test_cdc '''

import unittest

from src.station_status_cdc import get_latest_from_db
from src.station_status_cdc import get_data_from_api
from src.station_status_cdc import add_to_out_and_latest
from src.station_status_cdc import load_db
from src.models import Station_Status
from src.utils import get_session

###############
#   Globals   #
###############

DATABASE = 'test.db'
SESSION = get_session(DATABASE, echo=True)

##########################
#   Setup and Teardown   #
##########################


def setUp():
    empty_db()


def tearDown():
    empty_db()
    SESSION.close()

########################
#   Helper Functions   #
########################


def load_db_dummy_data(dummy):
    # insert test data into db
    SESSION.add(dummy)
    SESSION.commit()


def empty_db():
    # empty test db
    SESSION.query(Station_Status).delete()
    # SESSION.query(Load_Metadata).delete()
    SESSION.commit()


def create_dummy_data(seed=1):
    ''' use different seed to create different dummy '''
    ss = {'last_updated': 123456,
          'station_id': 999 * seed,
          'num_bikes_available': 100 * seed,
          'num_docks_available': 200 * seed,
          'is_installed': True,
          'is_renting': False,
          'is_returning': True,
          'last_reported': 78910,
          'num_bikes_disabled': 6
          }
    ss = Station_Status(ss)
    return ss

#############
#   Tests   #
#############


class CdcTestCase(unittest.TestCase):

    def test_get_latest_from_db(self):
        ''' load a dummy, then load a dummy with a change
            ensure changed is only one returned'''

        dummy1 = create_dummy_data()
        load_db_dummy_data(dummy1)
        dummy2 = create_dummy_data()
        dummy2.last_updated += 100
        load_db_dummy_data(dummy2)
        latest = get_latest_from_db(SESSION)
        l = latest[dummy2.station_id]
        self.assertEqual(l, dummy2)
        empty_db()

    def test_get_data_from_api(self):
        ''' ensure request returns json object with data '''
        lkp = get_data_from_api()
        self.assertTrue(len(lkp) > 0)

    def test_add_to_out_and_latest(self):
        ''' add a dummy to latest dict
            edit a copy, and then add_to_out_and_latest
            ensure out has copy, and latest_list is
            actually latest '''
        latest = {}
        out = []
        dummy = create_dummy_data()
        latest[dummy.station_id] = dummy
        dummy2 = create_dummy_data()
        dummy2.last_updated = 999
        add_to_out_and_latest(dummy2, out, latest)
        self.assertTrue(out[0] == dummy2 and
                        latest[dummy.station_id] == dummy2)

    def test_load_db(self):
        ''' put a dummy record into list
            and pass list into load_db() '''
        out = []
        dummy = create_dummy_data()
        out.append(dummy)
        load_db(out, SESSION)
        # need to recreate dummy since load_db clears it
        dummy = create_dummy_data()
        lkp = SESSION.query(Station_Status).first()
        self.assertEqual(dummy, lkp)
        empty_db()

    def test_equal(self):
        dummy = create_dummy_data()
        dummy2 = create_dummy_data()
        self.assertEqual(dummy, dummy2)

    def test_not_equal(self):
        dummy1 = create_dummy_data()
        dummy2 = create_dummy_data(2)
        self.assertNotEqual(dummy1, dummy2)


if __name__ == '__main__':
    unittest.main()
