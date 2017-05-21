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

db = 'bikeshare.db'

def get_data(file):
	'''
	uses requests to lookup bikeshare data
	file is one of the json data file names (no extension)
	--file is also the name of the target table
	(see https://github.com/NABSA/gbfs/blob/master/gbfs.md#files)
	ex: 'system_information'

	returns dict with data and name
	data is a list of dicts (different structure depending on file)
	name is name of file which is passed in here
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
	return {'data':data, 'name':file}

def add_hash_to_data(data):
	'''
	data is dict from json response
	should have at least keys 'data','name','last_updated'

	this function iterates through each dict,
	concatenates the values in the same order for each dict
	(when concatenating non-strings are converted to strings)
	calcualtes md5 of concatenated values
	and appends to the dict passed in
	'''

	#need to sort the keys in the dict so each concat is in the same order
	#just get the keys from the first dict in the list
	#TODO make sure each dict has the same keys (ie not no key if null)
	sorted_keys = sorted(data['data'][0].keys())

	#don't want last_updated included in md5 so remove from list
	sorted_keys.remove('last_updated')

	#empty dict to store concatenated strings
	concat_dict = {}

	#iterate through each dict (row) in the list (data['data'])
	for row in data['data']:
		# start each row with empty string
		concat_str = ''
		#loop through values in row and concat (convert non-string to string)
		for key in sorted_keys:
			concat_str += str(row[key])

		#get md5 of concat_str (md5 needs bytes type so use st.encode)
		concat_str_hashed = hashlib.md5(str.encode(concat_str)).hexdigest()
		#insert row into dict with id as key
		row['md5'] = concat_str_hashed

	#return data


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

def unpack_data(data):
	'''
	Takes json response data that we've added md5 to
	And unpacks into a list that will be friendly
	for loading into sqlite
	'''
	#need a list like: last_updated, region_id, name, region_md5
	unpacked = []
	if data['name'] == 'system_regions':
		for row in data['data']:
			unpacked.append((row['last_updated'],row['region_id'],
				row['name'],row['md5']))
	return unpacked


def load_system_regions(data):
	'''
	I think we need to load each table seperately
	since each has different structure
	'''
	unpacked = unpack_data(data)
	connection = sqlite3.connect(db)
	sql = 'INSERT INTO {} VALUES(?,?,?,?)'.format(data['name'])
	connection.executemany(sql,unpacked)
	connection.commit()
	connection.close()

def load_data(data):
	'''
	load data into db
	'''
	# need to has data before loading
	return None


def main():
	regions = get_data('system_regions')
	add_hash_to_data(regions)
	load_system_regions(regions)
	#return unpack_data(regions)

if __name__ == '__main__':
	main()