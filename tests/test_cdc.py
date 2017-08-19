''' tests.test_cdc '''

import unittest
import json
from os import environ
from time import time, sleep

from sqlalchemy import delete
import psycopg2

from src.station_status_cdc import get_data_from_api, load_db
from src.station_status_cdc import get_latest_from_db, get_changed_data
from src.bikeshare_etl import etl
from src.models import Station_Status, Station_Information
from src.models import System_Region, Load_Metadata
from src.utils import get_session


##########################
#   Setup and Teardown   #
##########################

def setUp():
    session = get_session(env='TST', echo=True)
    empty_db(session)
    # load to satisfy fk constraints
    load_regions_and_stations(session)
    session.close()


def tearDown():
    session = get_session(env='TST', echo=True)
    empty_db(session)
    session.close()


########################
#   Helper Functions   #
########################

def load_regions_and_stations(session):
    etl(System_Region, session)
    etl(Station_Information, session)


def empty_db(session):
    # empty test db
    session.query(Station_Status).delete()
    session.query(Station_Information).delete()
    session.query(System_Region).delete()
    session.query(Load_Metadata).delete()
    session.commit()


def empty_station_status_table(session):
    session.query(Station_Status).delete()
    session.commit()


def create_dummy_data(tstmp=0):
    ''' use different seed to create different dummy '''
    ss = {'last_updated': tstmp,
          'station_id': 1,
          'num_bikes_available': 100,
          'num_docks_available': 200,
          'num_bikes_disabled': 6,
          'is_installed': True,
          'is_renting': False,
          'is_returning': True,
          'last_reported': tstmp+10
          }
    ss = Station_Status(ss)
    return ss


def load_single_status_row(row, session):
    session.add(row)
    session.commit()


def get_connection_and_cursor(user='tst', pw=''):
    ''' use psycopgs2 to connect to test db'''
    if user == 'tst':
        u = environ['POSTGRES_USER_TST']
        pw = environ['POSTGRES_PW_TST']
    else:
        u = user
        pw = pw
    host = 'localhost'
    port = '5432'
    db = 'bikeshare_tst'
    connection = psycopg2.connect(dbname=db,
                                  user=u,
                                  password=pw,
                                  host=host,
                                  port=port)
    return connection, connection.cursor()


def select_all_stmt():
    return ('''SELECT status_id, last_updated, station_id, num_bikes_available,
            num_bikes_disabled, num_docks_available, num_docks_disabled,
            is_installed, is_renting, is_returning, last_reported, modified_by
            FROM public.station_status
            ORDER BY station_id, last_updated;''')


def get_json_from_file(filename):
    with open(filename, 'r') as json_file:
        return json.load(json_file)


#############
#   Tests   #
#############

class CdcTestCase(unittest.TestCase):

    def test_get_data_from_api(self):
        '''API request should return dict with keys: data, ttl, last_updated'''
        lkp = get_data_from_api()
        keys = ['data', 'ttl', 'last_updated']
        keys_in_lkp = True
        for i in keys:
            if i not in lkp:
                keys_in_lkp = False
        self.assertTrue(keys_in_lkp)

    def test_load_data_station_status(self):
        ''' Load data, then pull it from db and ensure match '''
        session = get_session(env='TST', echo=False)
        data = get_data_from_api()
        print(f'rows pulled: {len(data["data"]["stations"])}')
        # tstmp needed for creating Station_Status obj
        last_updated = data['last_updated']
        out = []
        comp = {}
        for row in data['data']['stations']:
            row['last_updated'] = last_updated
            record = Station_Status(row)
            out.append(record)
            comp[record.station_id] = record
        load_db(out, session)
        # connect and get all data from db
        conn, cur = get_connection_and_cursor()
        cur.execute(select_all_stmt())
        db_data = cur.fetchall()
        all_match = True
        i = 0
        if len(db_data) != len(comp):
            all_match = False
            print('lens dont match.')
            print(f'db: {len(db_data)}')
            print(f'comp: {len(comp)}')
        while i < len(db_data) and all_match:
            row = db_data[i]
            if row != comp[row[2]].to_tuple():
                all_match = False
            i += 1
        self.assertTrue(all_match)
        empty_station_status_table(session)
        session.close()

    def test_get_latest_data(self):
        ''' Ensure get_latest_data_from_db brings back actual latest '''
        session = get_session(env='TST', echo=True)
        # create a dummy record and load it
        d1 = create_dummy_data(time())
        load_single_status_row(d1, session)
        # create another dummy with a later tstmp
        d2 = create_dummy_data(time()+10)
        load_single_status_row(d2, session)
        # get latest (dict with station_id as key)
        latest = get_latest_from_db(session)
        self.assertEqual(d2, latest[d2.station_id])
        empty_station_status_table(session)
        session.close()

    def test_comparison(self):
        ''' Ensure only changed data is loaded.'''
        session = get_session(env='TST', echo=True)
        # load first dummy file on empty db
        data_d1 = get_json_from_file('tests/dummy1.json')
        last_updated = data_d1['last_updated']
        d1_objs = []
        for row in data_d1['data']['stations']:
            row['last_updated'] = last_updated
            session.add(Station_Status(row))
        session.commit()
        # compare and load file 2
        latest = get_latest_from_db(session)
        data_d2 = get_json_from_file('tests/dummy2.json')
        out, latest = get_changed_data(data_d2, latest)
        load_db(out, session)
        # compare and load file 3
        data_d3 = get_json_from_file('tests/dummy3.json')
        out, latest = get_changed_data(data_d3, latest)
        load_db(out, session)
        # pull all data from db.
        # should match exactly data in desired file
        conn, cur = get_connection_and_cursor()
        cur.execute(select_all_stmt())
        db_data = cur.fetchall()
        desired = get_json_from_file('tests/desired.json')
        all_match = True
        if len(db_data) != len(desired['stations']):
            all_match = False
            print(f"{db_data} should be same len as {desired['stations']}")
            print(f"{len(db_data)} != {len(desired['stations'])}")
        i = 0
        while i < len(db_data) and all_match:
            row = db_data[i]
            should_be = Station_Status(desired['stations'][i]).to_tuple()
            # not worried about first and last vals (auto id and username)
            print(row)
            print(should_be)
            all_match = row[1:-1] == should_be[1:-1]
            if not all_match:
                print(f'row {row} should match row {should_be}')
                print('except first val which is auto incremented id')
            i += 1
        self.assertTrue(all_match)
        empty_station_status_table(session)
        session.close()

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
