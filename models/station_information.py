# models/station_information.py

import hashlib

class Station_Information():



	def __init__(self,record):
		'''
		record should be a dict with the following keys:
		last_updated, station_id, name, short_name, lat, lon, region_id,
		rental_methods, capacity, eightd_has_key_dispenser

		region_md5 is calculated using the hashlib md5
		'''

		self.last_updated = record['last_updated']
		#convert id to int
		self.station_id = int(record['station_id'])
		self.name = record['name']
		self.short_name = record['short_name']
		self.lat = record['lat']
		self.lon = record['lon']
		self.region_id = record['region_id']
		self.capacity = record['capacity']
		self.eightd_has_key_dispenser = record['eightd_has_key_dispenser']

		self.unpack_rental_methods(record['rental_methods'])
		self.set_station_md5()

		#TODO: to_list()


	def unpack_rental_methods(self,rental_methods):
		if 'KEY' in rental_methods:
			self.rental_methods_KEY = True
		else:
			self.rental_methods_KEY = False
		if 'CREDITCARD' in rental_methods:
			self.rental_methods_CREDITCARD = True
		else:
			self.rental_methods_CREDITCARD = False
		if 'PAYPASS' in rental_methods:
			self.rental_methods_PAYPASS = True
		else:
			self.rental_methods_PAYPASS = False
		if 'APPLEPAY' in rental_methods:
			self.rental_methods_APPLEPAY = True
		else:
			self.rental_methods_APPLEPAY = False
		if 'ANDROIDPAY' in rental_methods:
			self.rental_methods_ANDROIDPAY = True
		else:
			self.rental_methods_ANDROIDPAY = False
		if 'TRANSITCARD' in rental_methods:
			self.rental_methods_TRANSITCARD = True
		else:
			self.rental_methods_TRANSITCARD = False
		if 'ACCOUNTNUMBER' in rental_methods:
			self.rental_methods_ACCOUNTNUMBER = True
		else:
			self.rental_methods_ACCOUNTNUMBER = False
		if 'PHONE' in rental_methods:
			self.rental_methods_PHONE = True
		else:
			self.rental_methods_PHONE = False
		
	def set_station_md5(self):
		self.station_md5 = hashlib.md5(str.encode(str(self.region_id)\
										+ self.name)).hexdigest()