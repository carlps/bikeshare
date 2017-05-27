# models/station_information.py

import hashlib

class Station_Information():



	def __init__(self,record):
		'''
		record should be a dict with the following keys:
		last_updated, station_id, name, short_name, lat, lon, region_id,
		rental_methods, capacity, eightd_has_key_dispenser

		region_md5 is calculated using the hashlib md5

		any fields that aren't require use set_[] to prevent KeyError
		'''

		self.record = record

		self.last_updated = record['last_updated']
		#convert id to int
		self.station_id = int(record['station_id'])
		self.name = record['name']
		self.lat = record['lat']
		self.lon = record['lon']
		
		#optional fields use set_
		self.set_short_name()
		self.set_region_id()
		self.set_capacity()
		self.set_eightd_has_key_dispenser()

		

		self.unpack_rental_methods()
		self.set_station_md5()

	def set_short_name(self):
		try:
			self.short_name = self.record['short_name']
		except KeyError:
			self.short_name = None

	def set_region_id(self):
		try:
			self.region_id = self.record['region_id']
		except KeyError:
			self.region_id = None

	def set_capacity(self):
		try:
			self.capacity = self.record['capacity']
		except KeyError:
			self.capacity = None

	def set_eightd_has_key_dispenser(self):
		try:
			self.eightd_has_key_dispenser = self.record['eightd_has_key_dispenser']
		except KeyError:
			self.eightd_has_key_dispenser = None


	def unpack_rental_methods(self):
		try:
			self.rental_methods = self.record['rental_methods']
		except KeyError:
			# if rental methods not in dict, set all as null
			self.rental_method_KEY = None
			self.rental_method_CREDITCARD = None
			self.rental_method_PAYPASS = None
			self.rental_method_APPLEPAY = None
			self.rental_method_ANDROIDPAY = None
			self.rental_method_TRANSITCARD = None
			self.rental_method_ACCOUNTNUMBER = None
			self.rental_method_PHONE = None

		if 'KEY' in self.rental_methods:
			self.rental_method_KEY = True
		else:
			self.rental_method_KEY = False
		if 'CREDITCARD' in self.rental_methods:
			self.rental_method_CREDITCARD = True
		else:
			self.rental_method_CREDITCARD = False
		if 'PAYPASS' in self.rental_methods:
			self.rental_method_PAYPASS = True
		else:
			self.rental_method_PAYPASS = False
		if 'APPLEPAY' in self.rental_methods:
			self.rental_method_APPLEPAY = True
		else:
			self.rental_method_APPLEPAY = False
		if 'ANDROIDPAY' in self.rental_methods:
			self.rental_method_ANDROIDPAY = True
		else:
			self.rental_method_ANDROIDPAY = False
		if 'TRANSITCARD' in self.rental_methods:
			self.rental_method_TRANSITCARD = True
		else:
			self.rental_method_TRANSITCARD = False
		if 'ACCOUNTNUMBER' in self.rental_methods:
			self.rental_method_ACCOUNTNUMBER = True
		else:
			self.rental_method_ACCOUNTNUMBER = False
		if 'PHONE' in self.rental_methods:
			self.rental_method_PHONE = True
		else:
			self.rental_method_PHONE = False
		
	def set_station_md5(self):

		#############
		### WRONG ###
		###  FIX  ###
		#############
		self.station_md5 = hashlib.md5(str.encode(str(self.region_id)\
										+ self.name)).hexdigest()

	def to_list(self):
		'''
		last_updated, station_id, short_name, name, lat, lon, 
		capacity, region_id, eightd_has_ley_dispenser, rental_method_KEY, 
		rental_method_CREDITCARD, rental_method_PAYPASS, 
		rental_method_APPLEPAY, rental_method_ANDROIDPAY, 
		rental_method_TRANSITCARD, rental_method_ACCOUNTNUMBER, 
		rental_method_PHONE, station_md5, transtype, latest_row_ind

		use none_to_null() on optionals
		'''

		return [self.last_updated, 
				self.station_id, 
				self.none_to_null(self.short_name), 
				self.name, 
				self.lat, 
				self.lon, 
				self.none_to_null(self.capacity), 
				self.none_to_null(self.region_id), 
				self.none_to_null(self.eightd_has_key_dispenser), 
				self.none_to_null(self.rental_method_KEY), 
				self.none_to_null(self.rental_method_CREDITCARD), 
				self.none_to_null(self.rental_method_PAYPASS), 
				self.none_to_null(self.rental_method_APPLEPAY), 
				self.none_to_null(self.rental_method_ANDROIDPAY), 
				self.none_to_null(self.rental_method_TRANSITCARD), 
				self.none_to_null(self.rental_method_ACCOUNTNUMBER), 
				self.none_to_null(self.rental_method_PHONE),
				self.station_md5]

	def none_to_null(self,value):
		'''
		def if a value is None, return "NULL"
		else return value
		'''
		return value if value is not None else "NULL"

	def __repr__(self):
		return '<name {}>'.format(self.name)
