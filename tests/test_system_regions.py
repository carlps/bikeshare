'''
tests.test_system_regions
'''

import unittest
from os import environ

from sqlalchemy import delete
import psycopg2

from src.bikeshare_etl import get_data, compare_data
from src.models import Load_Metadata, System_Region
from src.utils import get_session

###############
#   Globals   #
###############

SESSION = get_session(env='TST', echo=True)

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
    SESSION.query(System_Region).delete()
    SESSION.query(Load_Metadata).delete()
    SESSION.commit()


def create_dummy_region(name='test_region'):
    sr = {'last_updated': 12345, 'region_id': '9999', 'name': name}
    sr = System_Region(sr)
    sr.transtype = 'I'
    return sr


def create_metadata(model):
    return Load_Metadata(model.__tablename__, SESSION)


def get_cursor():
    ''' use psycopgs2 to connect to test db'''
    u = environ['POSTGRES_USER_TST']
    pw = environ['POSTGRES_PW_TST']
    host = 'localhost'
    port = '5432'
    db = 'bikeshare_tst'
    connection = psycopg2.connect(dbname=db,
                                  user=u,
                                  password=pw,
                                  host=host,
                                  port=port)
    return connection.cursor()


#############
#   Tests   #
#############


class SystemRegionTestCase(unittest.TestCase):

    def test_get_data_returns_dict(self):
        ''' ensure we get a dict of System_Regions back '''
        metadata = create_metadata(System_Region)
        data = get_data(System_Region, metadata)
        self.assertIsInstance(data, dict)

    def test_get_data_dict_key_is_id(self):
        ''' ensure dict key is row.id'''
        metadata = create_metadata(System_Region)
        data = get_data(System_Region, metadata)
        correct = True
        for row in data:
            if row != data[row].id:
                correct = False
        self.assertTrue(correct)

    def test_load_on_empty(self):
        ''' all records pulled down should be loaded into db '''
        metadata = create_metadata(System_Region)
        data = get_data(System_Region, metadata)
        compare_data(data, System_Region, metadata, SESSION)
        SESSION.commit()
        # get data from db
        cur = get_cursor()
        cur.execute('''SELECT region_id,
                       region_name,
                       row_modified_tstmp,
                       load_id,
                       transtype,
                       modified_by
                       FROM system_regions''')
        db_data = cur.fetchall()
        correct = True
        # compare each val of each row from db to each row pulled from web
        i = 0
        while i < len(db_data) and correct:
            row = db_data[i]
            orig = data[row[0]].to_tuple()
            correct = (row == orig)
            if not correct:
                print(f'row {row} didnt match orig {orig}')
            i += 1
        self.assertTrue(correct)
        empty_db()

    def test_update_only_updates_that_record(self):
        ''' load, then load an update. ensure only that record was updated'''
        metadata = create_metadata(System_Region)
        data = get_data(System_Region, metadata)
        compare_data(data, System_Region, metadata, SESSION)
        SESSION.commit()
        originals = {}
        for row in data:
            originals[row] = data[row].to_tuple()
        m2 = create_metadata(System_Region)
        d2 = get_data(System_Region, m2)
        u_record = d2[list(d2.keys())[0]]
        u_record.region_name = 'phoney balogna'
        print(u_record)
        u_data = {u_record.id: u_record}
        compare_data(u_data, System_Region, metadata, SESSION)
        SESSION.commit()
        cur = get_cursor()
        cur.execute('''SELECT region_id,
                       region_name,
                       row_modified_tstmp,
                       load_id,
                       transtype,
                       modified_by
                       FROM system_regions''')
        db_data = cur.fetchall()
        row_updated = True
        rows_match = True
        correct_trans = True
        i = 0
        while i < len(db_data) and\
                row_updated and\
                rows_match and\
                correct_trans:
            row = db_data[i]
            if row[0] == u_record.id:
                orig = u_record.to_tuple()
                trans = 'U'
                row_updated = orig != row
            else:
                orig = originals[row[0]]
                trans = 'I'
                rows_match = (orig == row)
            correct_trans = (row[4] == trans)
            if not row_updated:
                print(f'row {row} shouldnt match orig {orig}')
            if not rows_match:
                print(f'row {row} didnt match orig {orig}')
            if not correct_trans:
                print(f'incorrect trans on row {row} -- should be {trans}')
            i += 1
        self.assertTrue(row_updated and rows_match and correct_trans)
        empty_db()


if __name__ == '__main__':
    unittest.main()
