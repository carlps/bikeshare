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


def compare_data(table_name, md5_col):
	'''
	check if md5 of new row exists in db
	if yes, remove from data since we don't
	want to care about duplicates
	'''
	#first get target data for comparison
	connection = sqlite3.connect(db)
	sql = 'SELECT {0} FROM {1} WHERE {0} = ?'.format(md5_col,table_name)
	target_data = connection.execute(sql).fetchall()

	return None


def load_data(data, table_name):
	'''
	load data into db
	data should be list of objects with attribute obj.listed
	which should be a list of the data in correct order for loading
	'''
	#1st need list of lists instead of list of objs
	records_list = []
	for row in data:
		records_list.append(row.listed)

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
	load_data(regions_list,'system_regions')

if __name__ == '__main__':
	main()