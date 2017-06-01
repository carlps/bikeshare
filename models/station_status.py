# models/station_status


class Station_Status():
	
	def __init__(self,record):
		'''
		record should be a dict with the following keys:
		last_updated, station_id, num_bikes_available, 
		num_bikes_disabled, num_docks_available, 
		num_docks_disabled, is_installed, is_renting,
		is_returning, last_reported

		this one is different than others since it doesn't calc md5

		any fields that aren't require use set_[] to prevent KeyError
		'''
		self.record = record

		self.last_updated = record['last_updated']
		self. station_id = record['station_id']
		self.num_bikes_available = record['num_bikes_available']
		self.num_docks_available = record['num_docks_available']
		self.is_installed = record['is_installed']
		self.is_renting = record['is_renting']
		self.is_returning = record['is_returning']
		self.last_reported = record['last_reported']


		#optionals
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

	def none_to_null(self,value):
		'''
		def if a value is None, return "NULL"
		else return value
		'''
		return value if value is not None else "NULL"

	def to_list(self):
		'''
		in list to insert sql.
		must be in order:last_updated, station_id, num_bikes_available, 
		num_bikes_disabled, num_docks_available, 
		num_docks_disabled, is_installed, is_renting,
		is_returning, last_reported

		use none_to_null for empty values
		'''
		return [self.last_updated,
				self.station_id,
				self.num_bikes_available,
				self.none_to_null(self.num_bikes_disabled),
				self.num_docks_available,
				self.none_to_null(self.num_docks_disabled),
				self.is_installed,
				self.is_renting,
				self.is_returning,
				self.last_reported]