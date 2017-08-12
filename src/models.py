# src.models

import hashlib
import time
from datetime import datetime

import sqlalchemy
from sqlalchemy import Column, Integer, String, Numeric, DateTime
from sqlalchemy import Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, synonym


Base = declarative_base()


class Dimension():

    def set_optional(self, attribute_str, record):
        ''' if a attribute is optional, check if the
            key is in the dict. If yes, set.
            If no, set to None '''

        if attribute_str in record:
            return record[attribute_str]
        else:
            return None

    def __repr__(self):
        ''' When session is commited, this no longer works.'''
        repr_str = f'<{type(self).__name__}(\n'
        for key in self.__dict__.keys():
            if key not in ['_sa_instance_state', 'record']:
                repr_str += f'\t{key}={self.__dict__[key]}\n'
        repr_str += ')>'
        return repr_str


class Station_Status(Base):
    ''' Represents the status of a station at a given time (last_updated)
        extends Base from sqlalchemy
        Fact Table which is often updated (Inserts only)
        See https://github.com/NABSA/gbfs/blob/
        master/gbfs.md#station_statusjson

        Has a many-to-one relationship with Station_Status'''

    __tablename__ = 'station_status'

    last_updated = Column(DateTime, primary_key=True)
    station_id = Column(Integer,
                        ForeignKey('station_information.station_id'),
                        primary_key=True)
    num_bikes_available = Column(Integer)
    num_bikes_disabled = Column(Integer)
    num_docks_available = Column(Integer)
    num_docks_disabled = Column(Integer)
    is_installed = Column(Boolean)
    is_renting = Column(Boolean)
    is_returning = Column(Boolean)
    last_reported = Column(DateTime)

    station_information = relationship("Station_Information",
                                       back_populates='station_statuses')

    # set unique constraint for primary key cols
    UniqueConstraint(last_updated, station_id, name="station_status_pk_unique")

    def __init__(self, record):
        ''' record should be a dict with the following keys:
            last_updated, station_id, num_bikes_available,
            num_bikes_disabled, num_docks_available,
            num_docks_disabled, is_installed, is_renting,
            is_returning, last_reported

            record can also be tuple (from sql).
            if tuple, get/set is different since no keys

            this one is different than others since it doesn't calc md5

            any fields that aren't require use set_[] to prevent KeyError '''
        self.record = record

        self.last_updated = record['last_updated']
        self.station_id = int(record['station_id'])
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

    def __eq__(self, other):
        ''' override equals operator to just
            see if non-date values match'''
        return (self.num_bikes_available == other.num_bikes_available and
                self.num_bikes_disabled == other.num_bikes_disabled and
                self.num_docks_available == other.num_docks_available and
                self.num_docks_disabled == other.num_docks_disabled and
                self.is_installed == other.is_installed and
                self.is_renting == other.is_renting and
                self.is_returning == other.is_returning and
                self.last_reported == other.last_reported)

    def __ne__(self, other):
        return not self.__eq__(other)


class Station_Information(Dimension, Base):
    ''' Represents a record of information about a station
        extends Base from sqlalchemy
        Descriptive data (slow changing dimension)
        Describes a Station (facts of stations are Station_Status)
        See https://github.com/NABSA/gbfs/blob/
        master/gbfs.md#station_informationjson

        Has a many-to-one relationship with System_Region
        Has a one-to-many relationship with Station_Status '''

    __tablename__ = 'station_information'

    station_id = Column(Integer,
                        primary_key=True,
                        unique=True,
                        autoincrement=False)
    short_name = Column(String(length=10))
    station_name = Column(String(length=100), nullable=False)
    lat = Column(Numeric)
    lon = Column(Numeric)
    capacity = Column(Integer)
    region_id = Column(Integer, ForeignKey('system_regions.region_id'))
    eightd_has_key_dispenser = Column(Boolean)
    rental_method_key = Column(Boolean)
    rental_method_creditcard = Column(Boolean)
    rental_method_paypass = Column(Boolean)
    rental_method_applepay = Column(Boolean)
    rental_method_androidpay = Column(Boolean)
    rental_method_transitcard = Column(Boolean)
    rental_method_accountnumber = Column(Boolean)
    rental_method_phone = Column(Boolean)
    row_modified_tstmp = Column(DateTime)
    load_id = Column(Integer, ForeignKey('load_metadata.load_id'))
    transtype = Column(String(length=1))

    load = relationship("Load_Metadata", back_populates='stations')
    region = relationship("System_Region", back_populates='stations')
    station_statuses = relationship("Station_Status",
                                    order_by=Station_Status.last_updated,
                                    back_populates='station_information')

    # create sqlalchemy synonyms to lookup easier
    id = synonym("station_id")

    def __init__(self, record):
        ''' Record should be a dict with the following keys:
            station_id, name, lat, lon.
            The following are optional dict keys:
            short_name, region_id, capacity, eightd_has_key_dispenser '''

        self.station_id = int(record['station_id'])
        self.station_name = record['name']
        self.lat = record['lat']
        self.lon = record['lon']
        self.short_name = self.set_optional('short_name', record)
        self.region_id = self.set_optional('region_id', record)
        self.capacity = self.set_optional('capacity', record)
        self.eightd_has_key_dispenser = self.set_optional(
            'eightd_has_key_dispenser', record)
        self.unpack_rental_methods(record)
        self.row_modified_tstmp = datetime.now()
        self.transtype = None

    def unpack_rental_methods(self, record):
        if 'rental_methods' in record.keys():
            rental_methods = record['rental_methods']
            self.rental_method_key =\
                'KEY' in rental_methods
            self.rental_method_creditcard =\
                'CREDITCARD' in rental_methods
            self.rental_method_paypass =\
                'PAYPASS' in rental_methods
            self.rental_method_applepay =\
                'APPLEPAY' in rental_methods
            self.rental_method_androidpay =\
                'ANDROIDPAY' in rental_methods
            self.rental_method_transitcard =\
                'TRANSITCARD' in rental_methods
            self.rental_method_accountnumber =\
                'ACCOUNTNUMBER' in rental_methods
            self.rental_method_phone =\
                'PHONE' in rental_methods
        else:
            # if rental methods not in dict, set all as null
            self.rental_method_key = None
            self.rental_method_creditcard = None
            self.rental_method_paypass = None
            self.rental_method_applepay = None
            self.rental_method_androidpay = None
            self.rental_method_transitcard = None
            self.rental_method_accountnumber = None
            self.rental_method_phone = None

    def __eq__(self, other):
        ''' Only compare non-metadata attributes.
            Convert lat and lon to floats to ensure correct comparison'''
        return self.station_id == other.station_id and\
            self.short_name == other.short_name and\
            self.station_name == other.station_name and\
            float(self.lat) == float(other.lat) and\
            float(self.lon) == float(other.lon) and\
            self.capacity == other.capacity and\
            self.region_id == other.region_id and\
            self.eightd_has_key_dispenser ==\
            other.eightd_has_key_dispenser and\
            self.rental_method_key == other.rental_method_key and\
            self.rental_method_creditcard ==\
            other.rental_method_creditcard and\
            self.rental_method_paypass == other.rental_method_paypass and\
            self.rental_method_applepay == other.rental_method_applepay and\
            self.rental_method_androidpay ==\
            other.rental_method_androidpay and\
            self.rental_method_transitcard ==\
            other.rental_method_transitcard and\
            self.rental_method_accountnumber ==\
            other.rental_method_accountnumber and\
            self.rental_method_phone == other.rental_method_phone

    def __ne__(self, other):
        return not self.__eq__(other)


class System_Region(Dimension, Base):
    ''' represents a System Region (dimension)
        extends Base from sqlalchemy
        System regions are used as descriptive data
        to categorize regions in the system
        See https://github.com/NABSA/gbfs/blob/
        master/gbfs.md#system_regionsjson

        Has one-to-many relationship to Station_Information.'''

    __tablename__ = 'system_regions'

    region_id = Column(Integer,
                       primary_key=True,
                       autoincrement=False,
                       unique=True)
    region_name = Column(String(length=50), nullable=False)
    row_modified_tstmp = Column(DateTime)
    load_id = Column(Integer, ForeignKey('load_metadata.load_id'))
    transtype = Column(String(length=1))

    stations = relationship("Station_Information",
                            order_by=Station_Information.station_id,
                            back_populates='region')
    load = relationship("Load_Metadata", back_populates='regions')

    # create sqlalchemy synonyms to lookup easier
    id = synonym("region_id")

    def __init__(self, record):
        ''' record should be a dict with the following keys:
            region_id, name '''
        self.region_id = int(record['region_id'])  # convert region_id to int
        self.region_name = record['name']
        self.row_modified_tstmp = datetime.now()
        self.transtype = None

    def __eq__(self, other):
        ''' when checking equal, only check id and name '''
        return self.region_id == other.region_id and\
            self .region_name == other.region_name

    def __ne__(self, other):
        return not self.__eq__(other)


class Load_Metadata(Base):
    ''' Control table used to capture metadata of a load.
        Instantiated with dataset name, the rest is set throughout
        the load process.
        One-to-Many relationship with dimensions'''

    __tablename__ = 'load_metadata'

    load_id = Column(Integer, primary_key=True, unique=True)
    dataset = Column(String(length=20))
    start_tstmp = Column(DateTime)
    end_tstmp = Column(DateTime)
    src_rows = Column(Integer)
    inserts = Column(Integer)
    updates = Column(Integer)

    regions = relationship("System_Region",
                           order_by=System_Region.region_id,
                           back_populates='load')

    stations = relationship("Station_Information",
                            order_by=Station_Information.station_id,
                            back_populates="load")

    def __init__(self, dataset, session):
        ''' A new metadata record should be
            instantiated with the name of the dataset
            start time will be inserted automatically.
            Session is used to insert the new record into the
            DB which will give the load_id. Remember to update
            the record when load is complete.'''
        self.dataset = dataset
        self.start_tstmp = datetime.now()
        self.end_tstmp = None
        self.src_rows = None
        self.inserts = None
        self.updates = None
        # add record to session and commit
        # this increments id which is needed during processing
        session.add(self)
        session.commit()

    def __repr__(self):
        return ('<Load_Metadata(\n'
                f'load_id={self.load_id}\n'
                f'dataset={self.dataset}\n'
                f'start_tstmp={self.start_tstmp}\n'
                f'src_rows={self.src_rows}'
                f'end_tstmp={self.end_tstmp}\n'
                f'inserts={self.inserts}\n'
                f'updates={self.updates}\n'
                ')>')
