'''
create new load_metadata table that has decimal datatype for times
insert old data into new table
drop old
rename new (remove appended "_NEW" from name)
'''
import os
import shutil
import sqlite3

create_load_metadata = '''CREATE TABLE load_metadata_NEW
							(last_updated_tstmp INTEGER,
							 dataset TEXT,
							 start_time REAL,
							 end_time REAL,
							 inserts INTEGER,
							 updates INTEGER,
							 deletes INTEGER,
							 PRIMARY KEY(last_updated_tstmp, dataset)
							)'''

move_old_data_into_new = '''INSERT INTO load_metadata_NEW
							SELECT * FROM load_metadata'''

drop_old_table = '''DROP TABLE load_metadata'''
rename_new_table = '''ALTER TABLE load_metadata_NEW RENAME TO load_metadata'''

def backup_file(file):
	'''
	makes a copy of file with .bkp appended to filename in current working dir
	returns full path to file
	'''
	fname = os.path.join(os.getcwd(),file)
	fname_bkp = f'{fname}.bkp'
	return shutil.copyfile(fname,fname_bkp)

def main():
	db = 'bikeshare.db'
	bkp = backup_file(db)
	print(f'backed up db to {bkp}')
	connection = sqlite3.connect(db)
	connection.execute(create_load_metadata)
	print('created new table')
	connection.execute(move_old_data_into_new)
	print('inserted old data into new')
	connection.execute(drop_old_table)
	print('dropped old')
	connection.execute(rename_new_table)
	print('renamed new. committing and closing...')
	connection.commit()
	connection.close()

if __name__ == '__main__':
	main()