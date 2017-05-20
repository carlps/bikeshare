'''
create sqlite db to store bikeshare data
will eventually move to cloud
'''
import sqlite3



create_system_regions = '''CREATE TABLE system_regions
							(last_updated INTEGER,
							 region_id INTEGER,
							 name TEXT,
							 region_md5 TEXT,
							 PRIMARY KEY(last_updated, region_id)
							)'''

create_station_information = '''CREATE TABLE station_information
								(last_updated INTEGER,
								 station_id INTEGER,
								 short_name TEXT,
								 name TEXT,
								 lat REAL,
								 lon REAL,
								 capacity INTEGER,
								 region_id INTEGER,
								 eightd_has_ley_dispenser REAL,
								 rental_method_KEY INTEGER,
								 rental_method_CREDITCARD INTEGER,
								 rental_method_PAYPASS INTEGER
								 rental_method_APPLEPAY INTEGER,
								 rental_method_ANDROIDPAY INTEGER,
								 rental_method_TRANSITCARD INTEGER,
								 rental_method_ACCOUNTNUMBER INTEGER,
								 rental_method_PHONE INTEGER,
								 station_md5 TEXT,
								 FOREIGN KEY(region_id) REFERENCES system_regions(region_id),
								 PRIMARY KEY(last_updated, station_id)
								 )'''

create_station_status = '''CREATE TABLE station_status
							(last_updated INTEGER,
							 station_id INTEGER,
							 num_bikes_available INTEGER,
							 num_bikes_disabled INTEGER,
							 num_docks_available INTEGER,
							 num_docks_disabled INTEGER,
							 is_installed INTEGER,
							 is_renting INTEGER,
							 is_returning INTEGER,
							 last_reported INTEGER,
							 FOREIGN KEY(station_id) REFERENCES station_information(station_id),
							 PRIMARY KEY(last_updated, station_id)
							)'''

def main():
	connection = sqlite3.connect('bikeshare.db')
	connection.execute(create_system_regions)
	connection.execute(create_station_information)
	connection.execute(create_station_status)
	connection.commit()
	connection.close()

if __name__ == '__main__':
	main()