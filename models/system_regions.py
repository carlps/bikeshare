# models/system_regions.py

import hashlib

class System_Region():

	def __init__(self,record):
		'''
		record should be a dict with the following keys:
		last_updated, region_id, name

		region_md5 is calculated using the hashlib md5
		'''
		self.last_updated = record['last_updated']
		#convert region_id to int
		self.region_id = int(record['region_id'])
		self.name = record['name']
		self.set_region_md5()



	def __repr__(self):
		return '<name {}>'.format(self.name)

	def set_region_md5(self):
		self.region_md5 = hashlib.md5(str.encode(str(self.region_id)\
									 + self.name)).hexdigest()

	def to_list(self):
		'''
		returns a list of the attributes in the following order:
		last_updated, region_id, name, region_md5
		used to to insert into db
		'''
		return [self.last_updated, self.region_id, self.name, self.region_md5]