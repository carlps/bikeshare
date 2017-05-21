# models/system_regions.py

import hashlib

class System_Region():

	def __init__(self,last_udpated,region_id,name,region_md5):
		self.last_udpated = last_udpated
		self.region_id = region_id
		self.name = name
		if region_md5 ==- None:
			self.region_md5 = get_region_md5()
		else:
			self.region_md5 = region_md5

	def __repr__(self):
		return '<name {}>'.format(self.name)

	def get_region_md5(self):
		return hashlib.md5(str.encode(str(self.region_id) + self.name)).hexdigest()