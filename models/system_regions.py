# models/system_regions.py

import hashlib

class System_Region():

	def __init__(self,record):
		'''
		record should be a dict with the following keys:
		last_updated, region_id, name

		region_md5 is calculated using the hashlib md5

		self.listed is a list in the following order:
		last_updated, region_id, name, region_md5
		used to to insert into db

		self.type should be in all models to ensure source/tgt is correct
		'''
		self.last_updated = record['last_updated']
		self.region_id = record['region_id']
		self.name = record['name']
		self.region_md5 = self.get_region_md5()

		self.as_list = [self.last_updated, self.region_id, 
					   self.name, self.region_md5]



	def __repr__(self):
		return '<name {}>'.format(self.name)

	def get_region_md5(self):
		return hashlib.md5(str.encode(str(self.region_id) + self.name)).hexdigest()