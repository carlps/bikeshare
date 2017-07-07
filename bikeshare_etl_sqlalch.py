'''
this script should get capital bikeshare data, validate, and load to db

data from https://gbfs.capitalbikeshare.com/gbfs/gbfs.json

The gbfs.json file lists what files are available for 
consumption. 

For now we're just going to use station info, station status and system regions

For more details, check https://github.com/NABSA/gbfs/blob/master/gbfs.md
'''

import requests
import sqlite3
import time

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

from models import Station_Status, Station_Information, System_Region

db = 'bikeshare.db'

	
def get_session():
	'''
	create db connection and sqlalchemy engine
	return a session to interact with db
	'''
	# grab the folder where this script lives
	basedir = os.path.abspath(os.path.dirname(__file__))
	DATABASE = 'bikeshare.db'
	# define the full path for the database
	DATABASE_PATH = os.path.join(basedir, DATABASE)

	engine = create_engine(f'sqlite:///{DATABASE_PATH}')
	Session = sessionmaker(bind=engine)
	return Session()

def get_data(table_name, metadata):
	'''
	uses requests to lookup bikeshare data
	table_name is one of the json data file names (no extension)
	--table_name is also the name of the target table
	(see https://github.com/NABSA/gbfs/blob/master/gbfs.md#files)
	ex: 'system_information'

	metadata is a list with metadata that is updated throughout the process
	in this function, the first slot in the list is given last_updated

	returns list of dicts -- each dict contains the row data
	last_updated is a unix timestamp stored as integer
	last_updated is constant across each row, so it's added into each dict
	'''

	#first, validate proper param
	valid_files = ('station_information','station_status','system_regions')
	if table_name not in valid_files:
		print("invalid table_name. should be one of:",valid_files)
		return

	#build url using param
	url = 'https://gbfs.capitalbikeshare.com/gbfs/en/{}.json'.format(table_name)

	#attempt to get url
	response = requests.get(url)

	#if not 200, print repsonse and return None
	if response.status_code != 200:
		print('invalid response:',response.status_code)
		return None

	#retrieve json and break into pieces needed
	response_json = response.json()
	last_updated = response_json['last_updated'] # unix timestamp
	
	#data is a dict with one object: a list of dicts
	#so break out that list to return
	if len(response_json['data']) != 1:
		print('data dict not 1')
		#TODO: should write to log
		return None
	#different files have different key for the one list, so use values()
	#and convert to list
	data = list(response_json['data'].values())[0] #take first element since there can be only one
	#add last_updated to each row in data
	for row in data:
		row['last_updated'] = last_updated

	print(f'{table_name}: extract done. {len(data)} rows of data')

	#updated metadata
	metadata[0] = last_updated

	#return list
	return data

def compare_data(data,table_name, metadata):
	'''
	lookup the data in sql view
	if db data exists in new data but doesn't match: update
	if db data doesn't exist in new data: insert
	if db data exists but not in new data: delete
	if db data exists and matches: do nothing

	if nothing in db data: insert all
	'''


	#hardcoding params due to table_name. maybe better way?
	if table_name == 'system_regions':
		row_id = 'region_id'
		md5_col = 'region_md5'
		row_ids = [row.region_id for row in data]
	elif table_name == 'station_information':
		row_id = 'station_id'
		md5_col = 'station_md5'
		row_ids = [row.station_id for row in data]
	else:
		print('only works for system regions and station information. \
			   did you mess something up')
		return None

	#view contains latest, non-deleted data
	view_name = 'v_{}'.format(table_name)

	#pull last_updated from a row to use for deletes
	last_updated = data[0].last_updated

	#create sql statement for inserts/updates
	#since unknown number of parms will be passed, leave open
	iu_sql = 'SELECT {0}, {1} FROM {2} WHERE {0} IN (?'.format(
			row_id,md5_col,view_name)
	#calculate how many ?s to add for parms
	parms = ',?' * (len(row_ids)-1) + ')'
	iu_sql += parms

	#delete statement is inverse of insert/update statement
	#except we want entire record to insert a copy
	d_sql = 'SELECT * FROM {1} WHERE {0} NOT IN (?'.format(
			row_id,view_name)+parms

	#connect
	connection = sqlite3.connect(db)


	#get all ids and md5s with matching ids
	matches = connection.execute(iu_sql,row_ids).fetchall()

	# quick check - if matches has 0, set all to inserts and return
	if len(matches) == 0:
		print(f'{table_name}: all new records. {len(data)} inserts')
		return {'inserts':data,'updates':[],'updates_old':[],'deletes':[]}

	#convert list of sets to dict
	matches_dict = {match[0]:match[1] for match in matches}

	#we want whole records for deletes so we can insert D
	deletes = connection.execute(d_sql,row_ids).fetchall()

	connection.close()
	
	#prob should do if len(deletes) != 0
	#need to update delete timestamp
	deletes = [(last_updated,)+row[1:] for row in deletes]
	

	#again, probably a better way to do this, but for now this works
	if table_name == 'system_regions':
		#if id is not in matches_dict, then it is a brand new record
		inserts = [row for row in data\
					if row.region_id not in matches_dict]
		#remove inserts from data list (necessary for updates)
		data = [row for row in data if row not in inserts]
		
		# if md5s don't match, then the record has been updated
		# first get a copy (tuple of id,md5) of the pre-updated record
		# to update in db to latest_row_ind = N
		updates_old = [(row.region_id,matches_dict[row.region_id])for row in data\
						if row.region_md5 != matches_dict[row.region_id]]
		#then get whole mismatch record to insert as the latest, updated record
		updates = [row for row in data\
					if row.region_md5 !=matches_dict[row.region_id]]
		
	elif table_name == 'station_information':
		#if id is not in matches_dict, then it is a brand new record
		inserts = [data[data.index(row)] for row in data\
					 if row.station_id not in matches_dict]
		#remove inserts from data list (necessary for updates)
		data = [row for row in data if row not in inserts]
		
		
		#if md5s don't match, then the record has been updated
		#first get a copy of the pre-updated record to update in db to latest_row_ind = N
		updates_old = [(row.station_id,matches_dict[row.station_id]) for row in data\
						if row.station_md5 != matches_dict[row.station_id]]
		#then get whole mismatch record to insert as the latest, updated record
		updates = [data[data.index(row)] for row in data\
				 	if row.station_md5 !=matches_dict[row.station_id]]

	# update metadata
	metadata[4] = len(inserts)
	metadata[5] = len(updates)
	metadata[6] = len(deletes)

	print(f'{table_name}: compared data. {len(inserts)} inserts. ',
		f'{len(updates)} updates. {len(deletes)} deletes.')

	return({'inserts':inserts,'updates':updates,'updates_old':updates_old,'deletes':deletes})

def update_old(data,table_name):
	'''
	before inserting records that are U or D
	update the old version to set latest_row_ind = 'N'
	'''
	#if no deletes or updates, nothing to do here
	if len(data['deletes']+data['updates_old']) == 0:
		print('no updates or deletes')
		return
	#hardcoding params due to table_name. maybe better way?
	if table_name == 'system_regions':
		md5_col = 'region_md5'
	elif table_name == 'station_information':
		md5_col = 'station_md5'
	else:
		print('only works for system regions and station information. \
			   did you mess something up')
		return None

	#get md5s from deletes and updates_old to update
	md5s = [[rec[-1]] for rec in data['deletes']]
	md5s += [[rec[-1]] for rec in data['updates_old']]

	#use md5 to update
	sql = "UPDATE {0} SET latest_row_ind = 'N' WHERE {1} = (?)".format(
			table_name,md5_col)

	connection = sqlite3.connect(db)
	update = connection.executemany(sql,md5s)
	connection.commit()
	connection.close()

	print(f'{table_name}: updated {len(md5s)} old records.')


def load_data(data, table_name, metadata):
	'''
	load data into db
	data should be dict of list of objects with attribute obj.to_list()
	or for deletes, just list of values
	which should be a list of the data in correct order for loading

	dict keys can be 'inserts', 'updates', and/or 'deletes'
	'''
	
	#validate table_name
	if table_name not in ('system_regions','station_information','station_status'):
		print('invalid table name, bro')
		return
	if table_name in ('system_regions','station_information'):
		#get inserts and add 'I','Y' to end for transtype and latest_row_ind
		records_list = [record.to_list() + ['I','Y'] for record in data['inserts']]
		#same but 'U' for updates
		records_list += [record.to_list() + ['U','Y'] for record in data['updates']]
		#deletes are kinda different since they are not objects and have old date
		records_list += [list(record) + ['D','Y'] for record in data['deletes']]

		
	elif table_name == 'station_status':
		#station status is only inserts and table is formatted differently
		records_list = [record.to_list() for record in data]
		metadata[4] = len(records_list)

	#create SQL statement. 
	#since different data has different amount of cols, leave statement open
	sql_unfinished = 'INSERT INTO {} VALUES(?'.format(table_name)
	#then finish with length of first list -1 and close paren
	parms = ',?' * (len(records_list[0])-1) + ')'
	sql = sql_unfinished + parms

	metadata_sql = 'INSERT INTO load_metadata VALUES(?,?,?,?,?,?,?)'

	#connect
	connection = sqlite3.connect(db)
	connection.executemany(sql,records_list)
	#update metadata with end time
	metadata[3] = int(time.time())
	connection.execute(metadata_sql,metadata)
	connection.commit()
	connection.close()

	print(f'{table_name}: load done. loaded {len(records_list)} records.'\
		  f'\nsee table load_metadata, last_updated {metadata[0]} for details')


def etl(table_name):
	'''
	get data, transform, update old if needed,
	insert new
	'''
	#instantiate metadata list and set dataset, start time
	metadata = [0,table_name,int(time.time()),0,0,0,0]

	data = get_data(table_name, metadata)
	if(table_name == 'system_regions'):
		data = [System_Region(record) for record in data]
	elif(table_name == 'station_information'):
		data = [Station_Information(record) for record in data]
	elif(table_name == 'station_status'):
		data = [Station_Status(record) for record in data]

	#only compare and update for regions and station_information
	if table_name in ('system_regions','station_information'):
		data = compare_data(data, table_name, metadata)
		if((len(data['inserts'])+len(data['updates'])+len(data['deletes'])) == 0):
			print('no data to insert, update, or delete.')
			return
		update_old(data,table_name)

	load_data(data,table_name, metadata)

def main():
	etl('station_information')
	etl('system_regions')
	etl('station_status')


if __name__ == '__main__':
	main()