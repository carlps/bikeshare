'''
this script should get capital bikeshare data, validate, and load to db

data from https://gbfs.capitalbikeshare.com/gbfs/gbfs.json

The gbfs.json file lists what files are available for 
consumption. 

For now, we're just going to use station info and station status

For more details, check https://github.com/NABSA/gbfs/blob/master/gbfs.md
'''

import requests
import hashlib
import sqlite3

from models.system_regions import System_Region

db = 'bikeshare.db'

def get_data(file):
	'''
	uses requests to lookup bikeshare data
	file is one of the json data file names (no extension)
	--file is also the name of the target table
	(see https://github.com/NABSA/gbfs/blob/master/gbfs.md#files)
	ex: 'system_information'

	returns list of dicts -- each dict contains the row data
	last_updated is a unix timestamp stored as integer
	last_updated is constant across each row, so it's added into each dict
	'''

	#first, validate proper param
	valid_files = ('station_information','station_status','system_regions')
	if file not in valid_files:
		print("invalid file name. should be one of:",valid_files)
		return None

	#build url using param
	url = 'https://gbfs.capitalbikeshare.com/gbfs/en/{}.json'.format(file)

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
	#return dict
	return data

def compare_data(data,table_name):
	'''
	lookup md5 in table. if exists, remove from data
	'''
	#empty list for md5s
	row_ids = []

	#hardcoding params due to table_name. maybe better way?
	if table_name == 'system_regions':
		row_id = 'region_id'
		md5_col = 'region_md5'
		for row in data:
			row_ids.append(row.region_id)
	elif table_name == 'station_information':
		row_id = 'station_id'
		md5_col = 'station_md5'
		for row in data:
			row_ids.append(row.station_id)
	else:
		print('only works for system regions and station information. \
			   did you mess something up')
		return None

	#create sql statement
	#since unknown number of parms will be passed, leave open
	sql = 'SELECT {0}, {1} FROM {2} WHERE {0} IN (?'.format(
			row_id,md5_col,table_name)
	#calculate how many ?s to add for parms
	parms = ',?' * (len(row_ids)-1) + ')'
	sql += parms

	#connect
	connection = sqlite3.connect(db)

	#get all ids and md5s with matching ids
	lkps = connection.execute(sql,row_ids).fetchall()
	#convert list of sets to dict
	lkps_dict = {lkp[0]:lkp[1] for lkp in lkps}

	connection.close()

	#again, probably a better way to do this, but for now this works
	if table_name == 'system_regions':
		#if id is not in lkps_dict, then it is a brand new record
		inserts = [data.pop(data.index(row)) for row in data if row.region_id not in lkps_dict]
		#if md5s don't match, then the record has been updated
		updates = [data.pop(data.index(row)) for row in data if row.region_md5 !=lkps_dict[row.region_id]]
		
	elif table_name == 'station_information':
		#if id is not in lkps_dict, then it is a brand new record
		inserts = [data.pop(data.index(row)) for row in data if row.station_id not in lkps_dict]
		#if md5s don't match, then the record has been updated
		updates = [data.pop(data.index(row)) for row in data if row.station_md5 !=lkps_dict[row.station_id]]

	return({'inserts':inserts,'updates':updates})


def load_data(data, table_name):
	'''
	load data into db
	data should be list of objects with attribute obj.as_list
	which should be a list of the data in correct order for loading
	'''
	#1st need list of lists instead of list of objs
	records_list = []
	for row in data:
		records_list.append(row.as_list)

	#create SQL statement. 
	#since different data has different amount of cols, leave statement open
	sql_unfinished = 'INSERT INTO {} VALUES(?'.format(table_name)
	#then finish with length of first list -1 and close paren
	parms = ',?' * (len(records_list[0])-1) + ')'
	sql = sql_unfinished + parms

	#connect
	connection = sqlite3.connect(db)
	connection.executemany(sql,records_list)
	connection.commit()
	connection.close()

	return None


def main():
	regions = get_data('system_regions')
	regions_list = []
	for region in regions:
		regions_list.append(System_Region(region))

	upserts = compare_data(regions_list, 'system_regions')
	#load_data(regions_list,'system_regions')

if __name__ == '__main__':
	main()