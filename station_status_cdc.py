#station_status_cdc.py

import sqlite3
from time import time,sleep,strftime
import requests
from models.station_status import Station_Status

def get_latest_from_db(db):
	'''
	get latest station_status data from database
	returns list of 
	'''
	connection = sqlite3.connect(db)
	latest_sql = '''SELECT 
					station_id,
					last_updated,
					num_bikes_available,
					num_bikes_disabled,
					num_docks_available,
					num_docks_disabled,
					is_installed,
					is_renting,
					is_returning,
					last_reported
					FROM v_station_status'''
	latest = connection.execute(latest_sql).fetchall()
	#convert list of tuples to dict with station_id as key
	latest_dict = {}
	for row in latest:
		latest_dict[row[0]] = row[1:]
	return latest_dict


def get_data_api():
	'''
	get data from api
	make sure to return data, last_updated, and ttl
	do so in a way so comparison is easy to get_latest_from_db
	'''
	url = 'https://gbfs.capitalbikeshare.com/gbfs/en/station_status.json'
	response = requests.get(url)
	while response.status_code != 200: #if can't access api
		write_log(f'html response {response.status_code}')
		sleep(10)
		response = requests.get(url)
		#some sort of notice on error?

	return response.json()

def write_log(message):
	with open('cdc_log.txt','a') as file:
		file.write(f'{message}\t||\t{(strftime("%c"))}\n')


def compare(new_row,old_row,last_updated,out,latest_data):
	'''
	compare new and old
	if changed, add data to update and latest_data
	'''
	old_row = list(old_row) #convert from tuple to list
	new_row['last_updated'] = last_updated
	new_row = Station_Status(new_row).to_list()
	if new_row[2:-1] != old_row[1:-1]: #if changed
		#out is list to be inserted into db
		out.append(new_row)
		#replace old data in db_data dict with new
		#pop station ID from index 1 for key
		latest_data[int(new_row.pop(1))] = new_row


def load_db(out):
	'''
	load new data into db
	'''
	with open('test_out','a') as file:
		for row in out:
			file.write(str(row))

	print(f'updated {len(out)} rows.')


#######################################
#######################################
### LOOP IS UPDATING ALL EVERY TIME ###
#######################################
#######################################

def station_status_cdc(db):
	db_data = get_latest_from_db(db)
	nextFileTstmp = 0
	while True:
		out = [] #reset each loop
		if int(time()) >= nextFileTstmp:
			new_data = get_data_api()
			for new_row in new_data['data']['stations']:
				try:
					if new_row['last_reported'] != db_data[int(new_row['station_id'])][-1]: #last spot in latest_dict is last_reported
						compare(new_row,db_data[int(new_row['station_id'])],\
								new_data['last_updated'],out,db_data)
				except KeyError: 
					#key error means not in db_data so insert new record
					new_row['last_updated'] = new_data['last_updated']
					new_row = Station_Status(new_row).to_list()
					out.append(new_row)
					db_data[new_row.pop(1)] = new_row #add to latest
			load_db(out)  

			nextFileTstmp = new_data['last_updated'] + new_data['ttl']
			if nextFileTstmp-time() > 0:
				print(f'got data. sleepng for {nextFileTstmp-time()} seconds')
				sleep(nextFileTstmp-time())
			else:
				print(f'got data. no time to sleep, checking again.\nnextTstmp:{nextFileTstmp}\tnow:{time()}')
		else:
			print(f'sleeping {nextFileTstmp-time()} til next update')
			sleep(nextFileTstmp-time())

def main():
	db = 'bikeshare.db'
	station_status_cdc(db)


if __name__ == '__main__':
	main()