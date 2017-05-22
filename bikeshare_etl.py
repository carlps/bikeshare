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

def remove_duplicate_md5(data,table_name):
	'''
	lookup md5 in table. if exists, remove from data
	'''
	#empty list for md5s
	md5s = []

	#hardcoding params due to table_name. maybe better way?
	if table_name == 'system_regions':
		row_id = 'region_id'
		md5_col = 'region_md5'
		for row in data:
			md5s.append(row.region_md5)
	elif table_name == 'station_information':
		row_id = 'station_id'
		md5_col = 'station_md5'
		for row in data:
			md5s.append(row.station_md5)
	else:
		print('only works for system regions and station information. \
			   did you mess something up')
		return None

	#create sql statement	
	sql = 'SELECT {0} FROM {1} WHERE {2} = ?'.format(row_id,table_name,md5_col)

	#connect
	connection = sqlite3.connect(db)

	lkps = []
	for md5 in md5s:
		#lookup md5
		lkp_db = connection.execute(sql,(md5,)).fetchall()
		#lkp will be list of tuples (or empty list if no match)
		#so, for tuple in list
		for lkp in lkp_db:
			#append values in lkp to list
			lkps += lkp

	connection.close()

	#again, probably a better way to do this, but for now this works
	if table_name == 'system_regions':
		#return only rows that are not in lkp
		return [row for row in data if int(row.region_id) not in lkps]
	elif table_name == 'station_information':
		#return only rows that are not in lkp
		return [row for row in data if int(row.station_id) not in lkps]

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

	regions_list = remove_duplicate_md5(regions_list, 'system_regions')
	load_data(regions_list,'system_regions')

if __name__ == '__main__':
	main()