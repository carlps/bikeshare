'''
tests.test_Station_Information
'''

import unittest
import os
from time import time

from sqlalchemy import delete

from src.bikeshare_etl import get_data, compare_data, update_old, load_data, etl
from src.models import Load_Metadata, Station_Information
from src.utils import get_session

###############
### Globals ###
###############

DATABASE = 'test.db'
SESSION = get_session(DATABASE, echo=True)

##########################
### Setup and Teardown ###
##########################

def setUp():
	empty_db()

def tearDown():
	empty_db()
	SESSION.close()
	


########################
### Helper Functions ###
########################

def load_db_dummy_data(dummy):
	# insert test data into db
	SESSION.add(dummy)
	SESSION.commit()

def empty_db():
	# empty test db
	SESSION.query(Station_Information).delete()
	SESSION.query(Load_Metadata).delete()
	SESSION.commit()

def create_dummy_region(name='test_region'):
	sr = {'last_updated':12345,'region_id':'9999','name':name}
	sr = System_Regions(sr)
	sr.set_transtype_and_latest('I','Y')
	return sr

def create_dummy_station(name='test_station'):
	si = {  'last_updated':12345,
			'station_id':'9999',
			'name':name,
			'lat':123.45,
			'lon':543.21,
			'short_name':'short name',
			'region_id':9999,
			'capacity':10,
			'rental_methods':['KEY',"CREDITCARD","ANDROIDPAY"]
			}
	si = Station_Information(si)
	si.set_transtype_and_latest('I','Y')
	return si


def do_all_through_get_data():
	metadata = get_dummy_metadata()
	data = get_data(Station_Information,metadata)
	return data

def do_all_through_compare_data():
	metadata = get_dummy_metadata()
	data = do_all_through_get_data()
	data = compare_data(data,Station_Information,
						metadata,SESSION)
	return data

def do_all_through_update_old():
	data = do_all_through_compare_data()
	update_old(data,Station_Information,SESSION)
	# return data in case needed
	# even though update old doesn't change data
	return data

def do_all_through_load_data():
	metadata = get_dummy_metadata()
	data = get_data(Station_Information,metadata)
	data = compare_data(data,Station_Information,
						metadata,SESSION)
	update_old(data,Station_Information,SESSION)
	load_data(data,Station_Information,metadata,SESSION)


def get_dummy_metadata():
	m = Load_Metadata(Station_Information.__tablename__)
	m.last_updated_tstmp = time()
	return m

#############
### Tests ###
#############

class StationInformationTestCase(unittest.TestCase):

	def test_get_data_station_information(self):
		''' ensure we get a list of Station_Informations back '''
		data = do_all_through_get_data()
		self.assertIsInstance(data[0],Station_Information)

	def test_compare_station_information_empty_db(self):
		''' empty db, all downloaded data should be inserts '''
		data = do_all_through_get_data()
		len_inserts = len(data)
		print(len_inserts)
		if len_inserts == 0:
			# is no data pulled down, raise runtime error
			raise RuntimeError('No Station Information data pulled from API')
		metadata = get_dummy_metadata()
		data = compare_data(data,Station_Information,metadata,SESSION)
		print(data)
		self.assertEqual(len(data['inserts']),len_inserts)

	def test_compare_station_information_deletes(self):
		''' should load data to db, 
			then pull down data and compare, 
			dummy should be marked as D 
		'''
		dummy = create_dummy_station()
		load_db_dummy_data(dummy)
		data = do_all_through_compare_data()
		self.assertEqual(data['deletes'][0].station_id,dummy.station_id)
		# make sure to empty or else data will affect other tests
		empty_db()
		

	def test_compare_station_information_updates(self):
		''' should get data and load, 
			then get data again, edit, and compare
		'''
		do_all_through_load_data()
		data = do_all_through_get_data()
		data[0].md5 = 'something else'
		metadata = get_dummy_metadata()
		data = compare_data(data,Station_Information,metadata,SESSION)
		self.assertEqual(data['updates'][0].md5,'something else')
		empty_db()

	def test_update_old_station_information(self):
		''' should load data to db, 
			then get new data and update 
			former latest records should now be latest=N 
		'''
		do_all_through_load_data()
		data = do_all_through_get_data()
		data[0].md5 = 'something else'
		update_id = data[0].id
		metadata = get_dummy_metadata()
		data = compare_data(data,Station_Information,metadata,SESSION)
		update_old(data,Station_Information,SESSION)
		lkp = SESSION.query(Station_Information).\
				filter(Station_Information.latest_row_ind == 'N').first()
		self.assertEqual(update_id,lkp.id)
		empty_db()

	def test_update_old_no_updates_Station_Information(self):
		''' load data, get new data, compare
			then run update_old. select latest=N
			results should be 0 '''
		do_all_through_load_data()
		data = do_all_through_update_old()
		lkp = SESSION.query(Station_Information).\
				filter(Station_Information.latest_row_ind == 'N').all()
		self.assertEqual(len(lkp),0)
		empty_db()

	def test_load_data_Station_Information_insert(self):
		''' just pull down, compare, and load on empty db 
			keep a copy of pulled down before comparison
			set transtype and latest, then compare to loaded
			should be same'''
		data = do_all_through_get_data()
		untouched = data[:]
		metadata = get_dummy_metadata()
		data = compare_data(data,Station_Information,metadata,SESSION)
		update_old(data,Station_Information,SESSION)
		load_data(data,Station_Information,metadata,SESSION)

		for record in untouched:
			record.set_transtype_and_latest('Y','I')

		loaded = SESSION.query(Station_Information).all()
		self.assertEqual(untouched,loaded)
		empty_db()

	def test_load_data_Station_Information_insert_metadata(self):
		''' just pull down, compare, and load on empty db 
			keep a copy of pulled down before comparison
			set transtype and latest, then compare to loaded
			should be same'''
		data = do_all_through_get_data()
		inserts = len(data)
		metadata = get_dummy_metadata()
		data = compare_data(data,Station_Information,metadata,SESSION)
		update_old(data,Station_Information,SESSION)
		load_data(data,Station_Information,metadata,SESSION)

		meta = SESSION.query(Load_Metadata).first()
		self.assertTrue(inserts == meta.inserts and 
						meta.updates == 0 and 
						meta.deletes == 0)
		empty_db()

	def test_load_data_Station_Information_update(self):
		''' load data, then pull down data and
			edit a record before comparison'''
		do_all_through_load_data()
		data = do_all_through_get_data()
		data[0].md5 = 'i changed'
		data[0].last_updated = 987654321
		metadata = get_dummy_metadata()
		data = compare_data(data,Station_Information,metadata,SESSION)
		update_old(data,Station_Information,SESSION)
		load_data(data,Station_Information,metadata,SESSION)

		updated = SESSION.query(Station_Information).\
						filter(Station_Information.md5 == 'i changed').\
						filter(Station_Information.latest_row_ind == 'Y').first()

		original = SESSION.query(Station_Information).\
						filter(Station_Information.latest_row_ind == 'N').first()

		self.assertEqual(updated.id,original.id)
		empty_db()

	def test_load_data_Station_Information_update_metadata(self):
		''' load data, then pull down data and
			edit a record before comparison'''
		do_all_through_load_data()
		data = do_all_through_get_data()
		data[0].md5 = 'i changed'
		data[0].last_updated = 987654321
		metadata = get_dummy_metadata()
		tstmp = metadata.last_updated_tstmp
		data = compare_data(data,Station_Information,metadata,SESSION)
		update_old(data,Station_Information,SESSION)
		load_data(data,Station_Information,metadata,SESSION)

		meta = SESSION.query(Load_Metadata).\
						filter(Load_Metadata.last_updated_tstmp == \
							tstmp).first()

		self.assertTrue(meta.inserts == 0 and 
						meta.updates == 1 and 
						meta.deletes == 0)
		empty_db()

	def test_load_data_Station_Information_delete(self):
		'''	put dummy record in db, then run full load
			get deleted from db, should be same as dummy
			but transtype = D and latest = Y '''
		dummy = create_dummy_station()
		load_db_dummy_data(dummy)
		do_all_through_load_data()
		deleted = SESSION.query(Station_Information).\
							filter(Station_Information.transtype == 'D').first()
		dummy.set_transtype_and_latest('D','Y')
		self.assertEqual(dummy,deleted)
		empty_db()

	def test_load_data_Station_Information_delete_metadata(self):
		'''	put dummy record in db, then run full load
			get deleted from db, should be same as dummy
			but transtype = D and latest = Y '''
		dummy = create_dummy_station()
		load_db_dummy_data(dummy)
		do_all_through_load_data()
		meta = SESSION.query(Load_Metadata).first()
		self.assertTrue(meta.updates == 0 and 
						meta.deletes == 1)
		empty_db()		

	def test_etl_Station_Information(self):
		''' run full etl process for Station Information '''
		etl(Station_Information,SESSION)
		lkp = SESSION.query(Station_Information).all()
		self.assertTrue(len(lkp)>0)
		empty_db()

	def test_Station_Information_set_transtype_and_latest(self):
		dummy = create_dummy_station()
		dummy.set_transtype_and_latest('X','Z')
		t_l = (dummy.transtype,dummy.latest_row_ind)
		self.assertEqual(t_l,('X','Z'))

	def test_Station_Information_set_md5(self):
		t1 = create_dummy_station(name="test1")
		t2 = create_dummy_station(name="test2")
		self.assertNotEqual(t1.md5,t2.md5)

	def test_Station_Information_init_md5(self):
		t1 = create_dummy_station()
		self.assertIsNotNone(t1.md5)

	def test_metadata_time_station_information(self):
		''' end time should be > start time'''
		etl(Station_Information,SESSION)
		meta = SESSION.query(Load_Metadata).first()
		self.assertTrue(meta.start_time < meta.end_time)
		empty_db()

	def test_metadata_dataset_station_information(self):
		''' dataset should be tablename'''
		etl(Station_Information,SESSION)
		meta = SESSION.query(Load_Metadata).first()
		self.assertEqual(meta.dataset,'station_information')
		empty_db()

	def test_metadata_last_updated_station_information(self):
		''' last updated from metadata should match loaded data'''
		etl(Station_Information,SESSION)
		lkp = SESSION.query(Station_Information).first()
		meta = SESSION.query(Load_Metadata).first()
		self.assertEqual(meta.last_updated_tstmp,lkp.last_updated)
		empty_db()


if __name__ == '__main__':
	unittest.main()