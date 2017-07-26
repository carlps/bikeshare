import hashlib
import time

import sqlalchemy
from sqlalchemy import Column, Integer, Text, Numeric, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, synonym


Base = declarative_base()

class Dimension():

	def set_transtype_and_latest(self,transtype,latest):
		self.transtype = transtype
		self.latest_row_ind = latest

	def set_md5(self, record):
		'''
		Record is a dictionary instance of an object.
		With the exception of last_updated, convert all attributes to
		string, and concatenate all into one string with no delimiter.
		Returns md5 hash of string of attributes
		'''

		record_string = ''
		
		for key in record.keys():
			if key != 'last_updated':
				record_string += str(record[key])

		return hashlib.md5(str.encode(record_string)).hexdigest()



class Station_Status(Base):
	'''
	Represents the status of a station at a given time (last_updated)
	extends Base from sqlalchemy
	Fact Table which is often updated (Inserts only)
	See https://github.com/NABSA/gbfs/blob/master/gbfs.md#station_statusjson

	Has a many-to-one relationship with Station_Status
	'''

	__tablename__ = 'station_status'

	last_updated = Column(Integer, primary_key=True)
	station_id = Column(Integer,\
						ForeignKey('station_information.station_id'),\
						primary_key=True)
	num_bikes_available = Column(Integer)
	num_bikes_disabled = Column(Integer)
	num_docks_available = Column(Integer)
	num_docks_disabled = Column(Integer)
	is_installed = Column(Boolean)
	is_renting = Column(Boolean)
	is_returning = Column(Boolean)
	last_reported = Column(Integer)

	station_information = relationship("Station_Information",\
										back_populates='station_statuses')

	def __init__(self,record):
		'''
		record should be a dict with the following keys:
		last_updated, station_id, num_bikes_available, 
		num_bikes_disabled, num_docks_available, 
		num_docks_disabled, is_installed, is_renting,
		is_returning, last_reported

		record can also be tuple (from sql).
		if tuple, get/set is different since no keys

		this one is different than others since it doesn't calc md5

		any fields that aren't require use set_[] to prevent KeyError
		'''
		self.record = record

		self.last_updated = record['last_updated']
		self. station_id = int(record['station_id'])
		self.num_bikes_available = record['num_bikes_available']
		self.num_docks_available = record['num_docks_available']
		self.is_installed = bool(record['is_installed'])
		self.is_renting = bool(record['is_renting'])
		self.is_returning = bool(record['is_returning'])
		self.last_reported = record['last_reported']
		# optionals
		self.set_num_bikes_disabled()
		self.set_num_docks_disabled()
		

	def set_num_bikes_disabled(self):
		try:
			self.num_bikes_disabled = self.record['num_bikes_disabled']
		except KeyError:
			self.num_bikes_disabled = None

	def set_num_docks_disabled(self):
		try:
			self.num_docks_disabled = self.record['num_docks_disabled']
		except KeyError:
			self.num_docks_disabled = None

	def __repr__(self):
		return ('<Station_Status(\n'
            f'\tlast_updated={self.last_updated},\n'
            f'\tstation_id={self.station_id},\n'
            f'\tnum_bikes_available={self.num_bikes_available},\n'
            f'\tnum_bikes_disabled={self.num_bikes_disabled},\n'
            f'\tnum_docks_available={self.num_docks_available},\n'
            f'\tnum_docks_disabled={self.num_docks_disabled},\n'
            f'\tis_installed={self.is_installed},\n'
            f'\tis_renting={self.is_renting},\n'
            f'\tis_returning={self.is_returning},\n'
            f'\tlast_reported={self.last_reported},\n'
            ')>')

	def is_different(self,other):
		'''
		compare this record with another record
		based on all attributes besides last_reported and station_id

		return true if the records don't match or false if they are the same
		'''
		if (self.num_bikes_available == other.num_bikes_available
				and self.num_bikes_disabled == other.num_bikes_disabled
				and self.num_docks_available == other.num_docks_available
				and self.num_docks_disabled == other.num_docks_disabled
				and self.is_installed == other.is_installed
				and self.is_renting == other.is_renting
				and self.is_returning == other.is_returning
				and self.last_reported == other.last_reported):
			#if all these values are the same, return false
			return False
		else: 
			return True


class Station_Information(Dimension, Base):
	'''
	Represents a record of information about a station
	extends Base from sqlalchemy
	Descriptive data (slow changing dimension)
	Describes a Station (facts of stations are Station_Status)
	See https://github.com/NABSA/gbfs/blob/master/gbfs.md#station_informationjson

	Has a many-to-one relationship with System_Region
	Has a one-to-many relationship with Station_Status
	'''

	__tablename__ = 'station_information'

	last_updated = Column(Integer, primary_key=True)
	station_id = Column(Integer, primary_key=True)
	short_name = Column(Text)
	name = Column(Text)
	lat = Column(Numeric)
	lon = Column(Numeric)
	capacity = Column(Integer)
	region_id = Column(Integer, ForeignKey('system_regions.region_id'))
	eightd_has_key_dispenser = Column(Numeric)
	rental_method_KEY = Column(Boolean)
	rental_method_CREDITCARD = Column(Boolean)
	rental_method_PAYPASS = Column(Boolean)
	rental_method_APPLEPAY = Column(Boolean)
	rental_method_ANDROIDPAY = Column(Boolean)
	rental_method_TRANSITCARD = Column(Boolean)
	rental_method_ACCOUNTNUMBER = Column(Boolean)
	rental_method_PHONE = Column(Boolean)
	station_md5 = Column(Text)
	transtype = Column(Text)
	latest_row_ind = Column(Text)
	
	region = relationship("System_Region", back_populates='stations')
	station_statuses = relationship("Station_Status",\
								order_by=Station_Status.last_updated,\
								back_populates='station_information')

	# create sqlalchemy synonyms to lookup easier
	id = synonym("station_id")
	md5 = synonym("station_md5")



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
		# convert id to int
		self.station_id = int(record['station_id'])
		self.name = record['name']
		self.lat = record['lat']
		self.lon = record['lon']
		
		# optional fields use set_
		self.set_short_name()
		self.set_region_id()
		self.set_capacity()
		self.set_eightd_has_key_dispenser()
		self.unpack_rental_methods()

		# calculate md5
		self.station_md5 = self.set_md5(record)

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
		# more elegant way?
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

	def __repr__(self):
		return ('<System_Information(\n'
		f'last_updated={self.last_updated},\n'
		f'\tstation_id={self.station_id},\n'
		f'\tshort_name={self.short_name},\n'
		f'\tname={self.name},\n'
		f'\tlat={self.lat},\n'
		f'\tlon={self.lon},\n'
		f'\tcapacity={self.capacity},\n'
		f'\tregion_id={self.region_id},\n'
		f'\teightd_has_key_dispenser={self.eightd_has_key_dispenser},\n'
		f'\trental_method_KEY={self.rental_method_KEY},\n'
		f'\trental_method_CREDITCARD={self.rental_method_CREDITCARD},\n'
		f'\trental_method_PAYPASS={self.rental_method_PAYPASS},\n'
		f'\trental_method_APPLEPAY={self.rental_method_APPLEPAY},\n'
		f'\trental_method_ANDROIDPAY={self.rental_method_ANDROIDPAY},\n'
		f'\trental_method_TRANSITCARD={self.rental_method_TRANSITCARD},\n'
		f'\trental_method_ACCOUNTNUMBER={self.rental_method_ACCOUNTNUMBER},\n'
		f'\trental_method_PHONE={self.rental_method_PHONE},\n'
		f'\tstation_md5={self.station_md5},\n'
		f'\ttranstype={self.transtype},\n'
		f'\tlatest_row_ind={self.latest_row_ind}'
		')>')


class System_Region(Dimension,Base):
	'''
	represents a System Region (dimension)
	extends Base from sqlalchemy
	System regions are used as descriptive data
	to categorize regions in the system
	See https://github.com/NABSA/gbfs/blob/master/gbfs.md#system_regionsjson

	Has one-to-many relationship to Station_Information.
	'''

	__tablename__ = 'system_regions'

	last_updated = Column(Integer, primary_key=True)
	region_id = Column(Integer, primary_key=True)
	name = Column(Text)
	region_md5 = Column(Text)
	transtype = Column(Text)
	latest_row_ind = Column(Text)

	stations = relationship("Station_Information",\
							order_by=Station_Information.station_id,\
							back_populates='region')

	# create sqlalchemy synonyms to lookup easier
	id = synonym("region_id")
	md5 = synonym("region_md5")

	def __init__(self,record):
		'''
		record should be a dict with the following keys:
		last_updated, region_id, name

		region_md5 is calculated using the hashlib md5
		'''
		self.last_updated = record['last_updated']
		self.region_id = int(record['region_id']) # convert region_id to int
		self.name = record['name']
		self.region_md5 = self.set_md5(record)

	def __repr__(self):
		return (f'<System_Region(last_updated={self.last_updated}, '
				f'region_id={self.region_id}, name={self.name}, '
				f'region_md5={self.region_md5})>')


class Load_Metadata(Base):
	'''
	Control table used to capture metadata of a load.
	Instantiated with dataset name, the rest is set throughout
	the load process.
	'''

	__tablename__ = 'load_metadata'

	last_updated_tstmp = Column(Integer, primary_key=True)
	dataset = Column(Text, primary_key=True)
	start_time = Column(Numeric)
	end_time = Column(Numeric)
	inserts = Column(Integer)
	updates = Column(Integer)
	deletes = Column(Integer)

	def __init__(self, dataset):
		'''
		a new metadata record should be instantiated with the name of the dataset
		start time will be inserted automatically
		'''
		self.dataset = dataset
		self.start_time = time.time()

	def __repr__(self):
		return ('<Load_Metadata(\n'
				f'last_updated_tstmp={self.last_updated_tstmp},\n'
				f'dataset={self.dataset}\n'
				f'start_time={self.start_time}\n'
				f'end_time={self.end_time}\n'
				f'inserts={self.inserts}\n'
				f'updates={self.updates}\n'
				f'deletes={self.deletes}\n'
				')>')