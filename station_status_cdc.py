#station_status_cdc.py

import json #temp for test files
import requests
from time import time,sleep,strftime

from sqlalchemy import and_
from sqlalchemy.sql import func
from sqlalchemy.orm import make_transient

from utils import get_session
from models import Station_Status


def get_latest_from_db(session):
	'''
	get latest station_status data from database
	returns dict with station_id as key and object as value
	'''
	#first define subquery which gets latest timestamp and ID
	subq = session.query(\
						func.max(Station_Status.last_updated).
							label('last_updated'),\
						Station_Status.station_id
						).group_by(Station_Status.station_id).subquery()

	#get latest by joining station_status with subquery
	latest = session.query(Station_Status).\
					join(subq,\
						and_(Station_Status.last_updated==subq.c.last_updated,\
							Station_Status.station_id==subq.c.station_id)\
						).all()
	latest_dict = {}
	for row in latest:
		# need to expunge and make transient because
		# we just want copies of the data, not connected to db
		session.expunge(row)
		make_transient(row)
		latest_dict[row.station_id] = row
	return latest_dict


def get_data_from_api():
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


def add_to_out_and_latest(new_row, out, latest_data):
	'''
	If a row is found to be new, run this to 
	add to output list as well as latest data dict.
	
	For latest data dict, will either replace old record
	or insert new record if that ID isn't already in there.
	'''
	out.append(new_row)
	make_transient(new_row)
	latest_data[new_row.station_id] = new_row


def load_db(out, session):
	'''
	load new data into db
	'''
	session.add_all(out)
	session.commit()
	for record in out:
		session.expunge(record)
		make_transient(record)


def station_status_cdc():
	session = get_session(echo=True)
	db_data = get_latest_from_db(session)
	nextFileTstmp = 0
	lastFileTstmp = 0
	# infinite loop
	while True:
		try:
			out = [] # reset output list with each loop
			# first check next file tstmp to ensure api is updated
			if int(time()) >= nextFileTstmp:
				new_data = get_data_from_api()
				# sometimes nextFileTstmp is off by a second or two
				# so ensure new data is actually new
				if new_data['last_updated'] != lastFileTstmp:
					for new_row in new_data['data']['stations']:
						# create a Station_Status object from new row
						new_row['last_updated'] = new_data['last_updated']
						new_obj = Station_Status(new_row)
						try:
							old_obj = db_data[new_obj.station_id]
						except KeyError: 
							# key error means not in db_data
							# aka a new record
							print(f'new row! id: {new_obj.station_id}')
							# add new record to output list and latest
							add_to_out_and_latest(new_row=new_obj,
												  out=out,
												  latest_data=db_data)

						# if rows are different
						if (new_obj.is_different(old_obj)):
							# add new to out and replace old in latest
							add_to_out_and_latest(new_row=new_obj,
												  out=out,
												  latest_data=db_data)
					if len(out) > 0: # if we have records to output
						load_db(out,session)
						# for testing, write out json to file:
						with open(f'test_out/{new_data["last_updated"]}.json',
									'w') as outfile:
							json.dump(new_data,outfile)

						print(f'updated {len(out)} rows at {time()} ' 
							  f'for {new_data["last_updated"]}')
					else:
						print('no changes. nothing to load ' 
							 f'for {new_data["last_updated"]}')

					lastFileTstmp = new_data['last_updated']
					nextFileTstmp = new_data['last_updated'] + new_data['ttl']

					# if comparison and load completed before new data is up
					if nextFileTstmp > int(time()):
						print('got data. sleepng for '
							 f'{nextFileTstmp-time()} seconds')
						# add 1 since file might not be there on first check
						sleep(nextFileTstmp-time()+1) 
					# if comparison and load completed 
					# after new data should be up
					else:
						print('got data. no time to sleep, '
							  'checking again.\n'
							 f'nextTstmp:{nextFileTstmp}\tnow:{time()}')
				# if loop restarted but data pulled is not new
				else:
					print('file not yet updated. sleeping 5.')
					sleep(5)
			# if loop started before next file should be up (needed?)
			else:
				print(f'sleeping {nextFileTstmp-time()} til next update')
				sleep(nextFileTstmp-time())
		except KeyboardInterrupt:
			session.close()
			print('\nsession closed')
			break


def main():
	station_status_cdc()


if __name__ == '__main__':
	main()