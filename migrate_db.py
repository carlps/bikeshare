'''
create new db with transtype in system_regions and station_information tables
moves data, sets transtypes to I, and renames old db to bikeshare.db.old
'''
import os
import sqlite3
import create_db

def get_new_data(table_name, old_db):
	'''
	get old data and add I to last column
	'''
	sql = 'SELECT * FROM {0}'.format(table_name)

	c = sqlite3.connect(old_db)

	old_data = c.execute(sql).fetchall()

	#add 'I' to the end for new data
	new_data = [row + ('I',) for row in old_data]

	c.close()

	return new_data

def insert_new_data(data,table_name,db):
	
	sql = 'INSERT INTO {0} VALUES(?'.format(table_name)
	sql +=',?' * (len(data[0])-1) + ')'

	c = sqlite3.connect(db)

	c.executemany(sql,data)

	c.commit()
	c.close()


def main():
	db = 'bikeshare.db'
	table_name = 'system_regions'
	new_system_regions = get_new_data(table_name,db)
	os.rename('bikeshare.db','bikeshare.db.old')
	create_db.main()
	insert_new_data(new_system_regions,table_name,db)

if __name__ == '__main__':
	main()