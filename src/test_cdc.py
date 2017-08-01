'''
test_cdc.py

for a given station_id:
	1 - get all rows from db
	2 - pull mathing row from json (based on last_updated)
	3 - ensure data is correct
'''

import sqlite3
import json
from models.station_status import Station_Status
db = 'bikeshare.db'

def get_db_rows(station_id):
	'''
	get ALL rows for a given station id
	return results which is a list of tuples of the data
	'''
	sql_stmt = 'SELECT * FROM station_status WHERE station_id = (?)'

	connection = sqlite3.connect(db)
	results = connection.execute(sql_stmt,(station_id,)).fetchall()
	connection.close()

	return results

def get_json(station_id,db_results):
	'''
	read in data from json files for specific records
	json file names are last_updated tstmps (col index 0)
	open appropriate json file and pull out record based on ID
	return list of dicts which are rows from json files for station_id
	'''

	json_list = []
	for row in db_results:
		with open(f'test_out/{row[0]}.json','r') as in_file:
			json_in = json.load(in_file)
		for record in json_in['data']['stations']:
			if record['station_id'] == str(station_id):
				json_list.append(record)
				break

	return json_list

def compare(db_results, json_data):
	'''
	use Station_Status object method to_list() to convert
	dict to list in correct order.
	for each row from db, ensure the exact row is in the sources
	then do the same for each row from source
	if anything doesn't match up, write to file and print to console
	'''
	source_list = []
	for row in json_data:
		source_list.append(Station_Status(row).to_list())

	failures = []
	for row in db_results:
		if list(row) not in source_list:
			print(f'{row} not in sources?')
			failures.append(row)

	for row in source_list:
		if tuple(row) not in db_results:
			print(f'{row} not in db?')
			failures.append(row)
	
	if len(failures) > 0:
		print(f'{len(failures)} failures for station_id {db_results[0][1]}')
		with open('test_cdc_failures','a') as out_file:
			out_file.write(str(failures))
	else:
		print(f'station_id: {db_results[0][1]} all good :)')

def get_all_station_ids():
	'''
	get all current station_id's from v_station_information
	'''
	sql_stmt = 'SELECT station_id FROM v_station_information'
	connection = sqlite3.connect(db)
	results = connection.execute(sql_stmt).fetchall()
	connection.close()
	ids_list = [id[0] for id in results]
	return ids_list

def main():
	for station_id in get_all_station_ids():
		db_rows = get_db_rows(station_id)
		json_data = get_json(station_id,db_rows)
		compare(db_rows,json_data)


if __name__ == '__main__':
	main()