'''
create views for latest non-deleted data
'''
import sqlite3

create_system_regions_view = '''CREATE VIEW v_system_regions AS
							SELECT
							last_updated,
							region_id,
							name,
							region_md5
							FROM system_regions
							WHERE latest_row_ind = 'Y'
							AND transtype != 'D'
							'''

create_station_information_view = '''CREATE VIEW v_station_information AS
								SELECT
								last_updated,
								station_id,
								short_name,
								name,
								lat,
								lon,
								capacity,
								region_id,
								eightd_has_key_dispenser,
								rental_method_KEY,
								rental_method_CREDITCARD,
								rental_method_PAYPASS,
								rental_method_APPLEPAY,
								rental_method_ANDROIDPAY,
								rental_method_TRANSITCARD,
								rental_method_ACCOUNTNUMBER,
								rental_method_PHONE,
								station_md5
								FROM station_information
								WHERE latest_row_ind = 'Y'
							 	AND transtype != 'D'
								'''

def main():
	connection = sqlite3.connect('bikeshare.db')
	connection.execute('DROP VIEW v_station_information')
	connection.execute('DROP VIEW v_system_regions')
	connection.execute(create_system_regions_view)
	connection.execute(create_station_information_view)
	connection.commit()
	connection.close()

if __name__ == '__main__':
	main()