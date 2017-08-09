'''
This script gets capital bikeshare data, validates against db, and loads

Data from https://gbfs.capitalbikeshare.com/gbfs/gbfs.json
For more details, check https://github.com/NABSA/gbfs/blob/master/gbfs.md

The gbfs.json file lists what files are available for consumption. 

For now we're just going to use station info, station status and system regions
There are models for each in models.py file

SQLAlchemy is used as an ORM
'''

import requests
import time
import os

from sqlalchemy.orm import make_transient

from .models import Station_Status, Station_Information, System_Region, Dimension, Load_Metadata
from .utils import get_session	

def get_data(model, metadata):
	'''
	uses requests to lookup bikeshare data
	model should be one of the main data models in models.py

	metadata is a database record with metadata for the load

	returns list of objects (each of type model)
	'''

	table_name = model.__tablename__

	# first, validate proper param
	valid_files = ('station_information','station_status','system_regions')
	if table_name not in valid_files:
		print("invalid table_name. should be one of:",valid_files)
		return

	# build url using param
	url = 'https://gbfs.capitalbikeshare.com/gbfs/en/{}.json'.format(table_name)

	# attempt to get url
	response = requests.get(url)

	# if not 200, print repsonse and return
	if response.status_code != 200:
		print('invalid response:',response.status_code)
		return

	# retrieve json and break into pieces needed
	response_json = response.json()
	last_updated = response_json['last_updated'] # unix timestamp
	
	# data is a dict with one object: a list of dicts
	# so break out that list to return
	if len(response_json['data']) != 1:
		print('data dict not 1')
		# TODO: HANDLE BETTER. should write to log
		return 
	# different files have different key for the one list, so use values()
	# and convert to list
	data = list(response_json['data'].values())[0]

	# add last_updated to each row in data, then convert each dict to the defined object.
	for row in data:
		row['last_updated'] = last_updated
		data[data.index(row)] = model(row)


	print(f'{model.__name__}: extract done. {len(data)} rows of data')

	# update metadata with last_updated
	metadata.last_updated_tstmp = last_updated

	# return list of objects
	return data

def compare_data(data,model,metadata,session):
	'''
	lookup the data in sql
	if db data exists in new data but doesn't match: update
	if db data doesn't exist in new data: insert
	if db data exists but not in new data: delete
	if db data exists and matches: do nothing

	if nothing in db data: insert all
	'''

	# get a list of row_ids to be used in comparison
	row_ids = [row.id for row in data]

	# get all ids and md5s with matching ids
	# filter for latest, non-deleted rows
	# store in dict with row_id as key and md5 as val
	matches = {}
	for id, md5 in session.query(model.id, model.md5).\
						filter(model.id.in_(row_ids)).\
						filter(model.latest_row_ind == 'Y').\
						filter(model.transtype != 'D').\
						all():
		matches[id] = md5

	# we want whole records for deletes so we can insert D
	# filter for region_id not in (note ~ in row_id.in_)
	deletes = session.query(model).\
						filter(~model.id.in_(row_ids)).\
						filter(model.latest_row_ind == 'Y').\
						filter(model.transtype != 'D').\
						all()

	# quick check - if no matches or deletes, 
	# set all records to inserts and return
	if len(matches) == 0 and len(deletes) == 0:
		for row in data:
			row.set_transtype_and_latest('I','Y')
		metadata.inserts = len(data)
		metadata.updates = 0
		metadata.deletes = 0
		print(f'{model.__name__}: all new records. {len(data)} inserts')
		return {'inserts':data,'updates':[],'updates_old':[],'deletes':[]}

	for row in deletes:
		# in order to prevent update when we want to insert a copy,
		# we have to expunge and make_transient delete rows
		session.expunge(row)
		make_transient(row)
		# then update transtype and latest_row_ind (see parent class Dimension)
		row.set_transtype_and_latest('D','Y')

	# pull last_updated from a row to use for deletes
	last_updated = data[0].last_updated
	for deleted_row in deletes:
		deleted_row.last_updated = last_updated

	# if id is not in matches, then it is a brand new record
	inserts = []
	for row in data:
		if row.id not in matches:
			# mark it as insert and latest
			row.set_transtype_and_latest('I','Y')
			# use pop to remove from data list
			inserts.append(data.pop(data.index(row)))

	# empty lists to fill in for updates
	updates = []
	updates_old = []

	# handle updates IF we have any matches
	if len(matches) > 0:	
		for row in data:
			# if md5s don't match, then the record has been updated
			if row.md5 != matches[row.id]:
				# tuple (id,md5) to update the old record to latest_row_ind=N
				updates_old.append((row.id,matches[row.id]))

				# whole new row to insert as latest, updated record
				row.set_transtype_and_latest('U','Y')
				updates.append(row)
	
	# update metadata
	metadata.inserts = len(inserts)
	metadata.updates = len(updates)
	metadata.deletes = len(deletes)

	print(f'{model.__name__}: compared data. {len(inserts)} inserts. ',
		f'{len(updates)} updates. {len(deletes)} deletes.')

	return({'inserts':inserts,'updates':updates,'updates_old':updates_old,'deletes':deletes})

def update_old(data,model,session):
	'''
	before inserting records that are U or D
	update the old version to set latest_row_ind = 'N'
	'''

	# if no deletes or updates, nothing to do here
	if len(data['deletes'] + data['updates_old']) == 0:
		print('no updates or deletes')
		return

	# get md5s from deletes and updates_old to update
	md5s = [rec.md5 for rec in data['deletes']]
	md5s += [rec[-1] for rec in data['updates_old']] # these are tuples with (id,md5)

	# use md5 to update old records
	session.query(model).\
			filter(model.md5.in_(md5s)).\
			update({model.latest_row_ind:'N'},synchronize_session='fetch')
	session.commit()

	print(f'{model.__name__}: updated {len(md5s)} old records.')

def load_data(data, model, metadata, session):
	'''
	load data into db
	data should be dict of lists of objects

	dict keys must be 'inserts', 'updates', and 'deletes'
	'''
	
	# combine all records into one batch
	batch = data['inserts'] + data['updates'] + data['deletes']

	# insert batch into db
	session.add_all(batch)
	metadata.end_time = time.time()
	session.add(metadata)
	session.commit()

	print(f'{model.__name__}: load done. loaded {len(batch)} records.'
		  f'\nsee table load_metadata, '
		  f'last_updated {metadata.last_updated_tstmp} for details')
	

def etl(model,session):
	'''
	get data, transform, update old if needed, insert new
	'''

	table_name = model.__tablename__

	# instantiate metadata object
	metadata = Load_Metadata(table_name)

	# get data from API
	data = get_data(model, metadata)
	
	# for dimensions, we have to compare and update old
	if issubclass(model,Dimension):
		data = compare_data(data, model, metadata, session)
		if((len(data['inserts'])+len(data['updates'])+len(data['deletes'])) == 0):
			print('no data to insert, update, or delete.')
			return
		update_old(data,model,session)
	else:
		print(f'{model} is not subclass of Dimension')
	load_data(data,model,metadata,session)

def main():
	session = get_session(db='bikeshare.db')
	etl(Station_Information,session)
	etl(System_Region,session)
	session.close()
	#etl('station_status')


if __name__ == '__main__':
	main()