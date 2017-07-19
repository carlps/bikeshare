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

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, make_transient

from models import Station_Status, Station_Information, System_Region, Dimension
	
def get_session():
	'''
	create db connection and sqlalchemy engine
	return a session to interact with db
	currently sqlite but not for long
	'''
	# grab the folder where this script lives
	basedir = os.path.abspath(os.path.dirname(__file__))
	DATABASE = 'bikeshare.db'
	# define the full path for the database
	DATABASE_PATH = os.path.join(basedir, DATABASE)

	engine = create_engine(f'sqlite:///{DATABASE_PATH}') # set echo=True in create_engine if debugging
	Session = sessionmaker(bind=engine)

	return Session()

def get_data(model, metadata):
	'''
	uses requests to lookup bikeshare data
	model should be one of the main data models in models.py

	metadata is a list with metadata that is updated throughout the process
	in this function, the first slot in the list is given last_updated
	--TODO: create a model for metadata

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

	# update metadata
	metadata[0] = last_updated

	# return list of objects
	return data

def compare_data(data,model,metadata):
	'''
	lookup the data in sql
	if db data exists in new data but doesn't match: update
	if db data doesn't exist in new data: insert
	if db data exists but not in new data: delete
	if db data exists and matches: do nothing

	if nothing in db data: insert all
	'''


	# quick check to ensure correct model
	if model not in (System_Region,Station_Information):
		print('only works for system regions and station information. \
			   did you mess something up')
		return

	# get a list of row_ids to be used in comparison
	row_ids = [row.id for row in data]

	# connect to db
	session = get_session()

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
	
	# quick check - if matches has 0, set all records to inserts and return
	if len(matches) == 0:
		print(f'{model.__name__}: all new records. {len(data)} inserts')
		return {'inserts':data,'updates':[],'updates_old':[],'deletes':[]}

	# we want whole records for deletes so we can insert D
	# filter for region_id not in (note ~ in row_id.in_)
	deletes = session.query(model).\
						filter(~model.id.in_(row_ids)).\
						filter(model.latest_row_ind == 'Y').\
						filter(model.transtype != 'D').\
						all()
	for row in deletes:
		# in order to prevent update when we want to insert a copy,
		# we have to expunge and make_transient delete rows
		session.expunge(row)
		make_transient(row)
		# then update transtype and latest_row_ind (see parent class Dimension)
		row.set_transtype_and_latest('D','Y')

	session.close()

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
	for row in data:
		# if md5s don't match, then the record has been updated
		if row.md5 != matches[row.id]:
			# tuple (id,md5) to update the old record to latest_row_ind=N
			updates_old.append((row.id,matches[row.id]))

			# whole new row to insert as latest, updated record
			row.set_transtype_and_latest('U','Y')
			updates.append(row)
	
	# update metadata
	metadata[4] = len(inserts)
	metadata[5] = len(updates)
	metadata[6] = len(deletes)

	print(f'{model.__name__}: compared data. {len(inserts)} inserts. ',
		f'{len(updates)} updates. {len(deletes)} deletes.')

	return({'inserts':inserts,'updates':updates,'updates_old':updates_old,'deletes':deletes})

def update_old(data,model):
	'''
	before inserting records that are U or D
	update the old version to set latest_row_ind = 'N'
	'''
	# ensure only correct models are passed
	if model not in (System_Region,Station_Information):
		print('only works for system regions and station information. \
			   did you mess something up')
		return

	# if no deletes or updates, nothing to do here
	if len(data['deletes'] + data['updates_old']) == 0:
		print('no updates or deletes')
		return

	# get md5s from deletes and updates_old to update
	md5s = [rec.md5 for rec in data['deletes']]
	md5s += [rec[-1] for rec in data['updates_old']] # these are tuples with (id,md5)

	# use md5 to update old records
	session = get_session()
	session.query(model).\
			filter(model.md5.in_(md5s)).\
			update({model.latest_row_ind:'N'},synchronize_session='fetch')
	session.commit()
	session.close()

	print(f'{model.__name__}: updated {len(md5s)} old records.')

def load_data(data, model, metadata):
	'''
	load data into db
	data should be dict of lists of objects

	dict keys must be 'inserts', 'updates', and 'deletes'
	'''
	
	# quick check to ensure correct model
	if model not in (System_Region,Station_Information):
		print('only works for system regions and station information. \
			   did you mess something up')
		return

	# combine all records into one batch
	batch = data['inserts'] + data['updates'] + data['deletes']

	# insert batch into db
	session = get_session()
	session.add_all(batch)
	# TODO load metadata as well
	session.commit()

	print(f'{model.__name__}: load done. loaded {len(batch)} records.'\
		  f'\nsee table load_metadata, last_updated {metadata[0]} for details')

def etl(model):
	'''
	get data, transform, update old if needed, insert new
	'''

	table_name = model.__tablename__

	# instantiate metadata list and set dataset, start time
	metadata = [0,table_name,int(time.time()),0,0,0,0]

	data = get_data(model, metadata)
	
	# for dimensions, we have to compare and update old
	if issubclass(model,Dimension):
		data = compare_data(data, model, metadata)
		if((len(data['inserts'])+len(data['updates'])+len(data['deletes'])) == 0):
			print('no data to insert, update, or delete.')
			return
		update_old(data,model)

	load_data(data,model,table_name)

def main():
	etl(Station_Information)
	etl(System_Region)
	#etl('station_status')


if __name__ == '__main__':
	main()