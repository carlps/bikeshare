''' tests.test_system_regions '''

import unittest
from os import environ

from sqlalchemy import delete
import psycopg2

from src.bikeshare_etl import get_data, compare_data, etl
from src.models import Load_Metadata, System_Region
from src.utils import get_session
from src.db.create_db_tst import create_db, drop_all_tables


##########################
#   Setup and Teardown   #
##########################

def setUp():
    create_db()
    session = get_session(env='TST', echo=True)
    session.close()


def tearDown():
    drop_all_tables()


########################
#   Helper Functions   #
########################

def empty_db(session):
    # empty test db
    session.query(System_Region).delete()
    session.query(Load_Metadata).delete()
    session.commit()


def create_dummy_region(name='test_region'):
    sr = {'region_id': '9999', 'name': name}
    sr = System_Region(sr)
    sr.transtype = 'I'
    return sr


def create_metadata(model, session):
    return Load_Metadata(model.__tablename__, session)


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


#############
#   Tests   #
#############


class SystemRegionTestCase(unittest.TestCase):

    def test_get_data_returns_dict(self):
        ''' ensure we get a dict of System_Regions back '''
        session = get_session(env='TST', echo=True)
        metadata = create_metadata(System_Region, session)
        data = get_data(System_Region, metadata)
        self.assertIsInstance(data, dict)
        session.close()

    def test_get_data_dict_key_is_id(self):
        ''' ensure dict key is row.id'''
        session = get_session(env='TST', echo=True)
        metadata = create_metadata(System_Region, session)
        data = get_data(System_Region, metadata)
        correct = True
        for row in data:
            if row != data[row].id:
                correct = False
        self.assertTrue(correct)
        session.close()

    def test_load_on_empty(self):
        ''' all records pulled down should be loaded into db '''
        session = get_session(env='TST', echo=True)
        metadata = create_metadata(System_Region, session)
        data = get_data(System_Region, metadata)
        compare_data(data, System_Region, metadata, session)
        session.commit()
        # get data from db
        conn, cur = get_connection_and_cursor()
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
        empty_db(session)
        session.close()

    def test_update_only_updates_that_record(self):
        ''' load, then load an update. ensure only that record was updated'''
        # first get data and load
        session = get_session(env='TST', echo=True)
        metadata = create_metadata(System_Region, session)
        data = get_data(System_Region, metadata)
        compare_data(data, System_Region, metadata, session)
        session.commit()
        # get tuple copies of each record that was loaded
        originals = {}
        for row in data:
            originals[row] = data[row].to_tuple()
        # now get data again
        m2 = create_metadata(System_Region, session)
        d2 = get_data(System_Region, m2)
        # get one record from data and make a change
        u_record = d2[list(d2.keys())[0]]
        u_record.region_name = 'phoney balogna'
        u_data = {u_record.id: u_record}
        # load new record (should be update)
        compare_data(u_data, System_Region, metadata, session)
        session.commit()
        # get all current data from db
        conn, cur = get_connection_and_cursor()
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
        # iterate through db data. break if any test fails
        i = 0
        while i < len(db_data) and\
                row_updated and\
                rows_match and\
                correct_trans:
            row = db_data[i]
            orig = originals[row[0]]
            if row[0] == u_record.id:
                trans = 'U'
                # update orig should not match row and ensure name was updated
                row_updated = (orig != row) and (row[1] == 'phoney balogna')
            else:
                # all other records row should match orig
                trans = 'I'
                rows_match = (orig == row)
            correct_trans = (row[-2] == trans)
            if not row_updated:
                print(f'row {row} shouldnt match orig {orig}')
                print('also, region_name in row should be phoney balogna')
            if not rows_match:
                print(f'row {row} didnt match orig {orig}')
            if not correct_trans:
                print(f'incorrect trans on row {row} -- should be {trans}')
            i += 1
        self.assertTrue(row_updated and rows_match and correct_trans)
        empty_db(session)
        session.close()

    def test_system_region_eq(self):
        ''' == operator on System Region should
            only compare region_name and region_id'''
        # create two identical records
        r1 = create_dummy_region()
        r2 = create_dummy_region()
        # change metadata that shouldn't be compared
        r1.load_id = 612
        r2.load_id = 9
        r1.transtype = 'P'
        r2.transtype = 'Q'
        self.assertEqual(r1, r2)

    def test_system_region_eq2(self):
        ''' Ensure == returns false when name and or id are different'''
        r1 = create_dummy_region(name='r1')
        r2 = create_dummy_region(name='r2')
        self.assertNotEqual(r1, r2)

    def test_update_captures_username(self):
        ''' When a db record is updated,
            modified_by should be changed to show who it was'''
        # run a load (as bikeshare_tst)
        session = get_session(env='TST', echo=True)
        etl(System_Region, session)
        conn, cur = get_connection_and_cursor()
        cur.execute('SELECT region_id FROM system_regions;')
        row_id = cur.fetchone()
        # connect as different user
        conn, cur = get_connection_and_cursor(user='test_user',
                                              pw=environ['POSTGRES_PW_TST'])
        cur.execute('''UPDATE system_regions
                       SET region_name = 'Beernalillo, NM'
                       WHERE region_id = (%s);''', row_id)
        conn.commit()
        cur.execute('SELECT region_id, modified_by FROM system_regions')
        db_data = cur.fetchall()
        correct = True
        i = 0
        while i < len(db_data) and correct:
            row = db_data[i]
            if row[0] == row_id[0]:
                user = 'test_user'
            else:
                user = environ['POSTGRES_USER_TST']
            correct = (row[1] == user)
            if not correct:
                print(f'user should be {user} but is actually {row[1]}')
            i += 1
        self.assertTrue(correct)
        empty_db(session)
        session.close()

if __name__ == '__main__':
    unittest.main()
