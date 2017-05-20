'''
this script should get capital bikeshare data
from https://gbfs.capitalbikeshare.com/gbfs/gbfs.json

The gbfs.json file lists what files are available for 
consumption. 

For now, we're just going to use station info and station status

For more details, check https://github.com/NABSA/gbfs/blob/master/gbfs.md
'''

import requests

def get_data(file):
	'''
	uses requests to lookup bikeshare data
	file is one of the json data file names (no extension)
	(see https://github.com/NABSA/gbfs/blob/master/gbfs.md#files)
	ex: 'system_information'

	returns dict with last_updated and data
	last_updated is a unix timestamp stored as integer
	data is a dict (different structure depending on file)
	'''
	if file=='gbfs':
		# gbfs file not in /en/ directory
		url='https://gbfs.capitalbikeshare.com/gbfs/{}.json'.format(file)
	else:
		# all others should be
		url = 'https://gbfs.capitalbikeshare.com/gbfs/en/{}.json'.format(file)
	response = requests.get(url)

	if response.status_code != 200:
		print('error code',response.status_code)
		return response.status_code # temporary

	response_json = response.json()

	last_updated = response_json['last_updated'] # unix timestamp

	data = response_json['data'] # dict of data

	return {'last_updated':last_updated,'data':data}

def main():
	pass

if __name__ == '__main__':
	main()