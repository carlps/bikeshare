import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

from models import Station_Information, System_Region, Station_Status

# grab the folder where this script lives
#basedir = os.path.abspath(os.path.dirname(__file__))

basedir = os.getcwd()
DATABASE = 'bikeshare.db'
# define the full path for the database

DATABASE_PATH = os.path.join(basedir, DATABASE)

engine = create_engine(f'sqlite:///{DATABASE_PATH}')

Session = sessionmaker(bind=engine)
session = Session()
'''
for region_id, name in session.query(System_Region.region_id, System_Region.name):
	print(region_id,name)

region40 = session.query(System_Region).filter(System_Region.region_id == 40).\
									filter(System_Region.latest_row_ind == 'Y').\
									one()
'''

station1 = session.query(Station_Information).\
						filter(Station_Information.station_id == 1).\
						filter(Station_Information.latest_row_ind == 'Y').\
						one()
