# src.models

import hashlib
import time
from datetime import datetime

import sqlalchemy
from sqlalchemy import Column, Integer, Text, Numeric, DateTime
from sqlalchemy import Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, synonym


Base = declarative_base()


class Dimension():

    def set_transtype_and_latest(self, transtype, latest):
        self.transtype = transtype
        self.latest_row_ind = latest

    def set_tstmps(self):
        ''' If a new record, set both timestamps to now.
            If an update, set row_update to now and leave
            row insert alone.'''
        if self.row_update_tstmp:
            # if the tstmp already has value, then we are updating
            self.row_update_tstmp = datetime.now()
        else:
            # if not, then set both to now
            self.row_insert_tstmp = datetime.now()
            self.row_update_tstmp = self.row_insert_tstmp

    def set_optional(self, attribute_str, record):
        ''' if a attribute is optional, check if the
            key is in the dict. If yes, set.
            If no, set to None '''

        if attribute_str in record:
            return record[attribute_str]
        else:
            return None


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
    short_name = Column(Text)
    name = Column(Text)
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
    row_insert_tstmp = Column(DateTime)
    row_update_tstmp = Column(DateTime)
    load_id = Column(Integer, ForeignKey('load_metadata.load_id'))

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
        self.name = record['name']
        self.lat = record['lat']
        self.lon = record['lon']
        self.short_name = self.set_optional('short_name', record)
        self.region_id = self.set_optional('region_id', record)
        self.capacity = self.set_optional('capacity', record)
        self.eightd_has_key_dispenser = self.set_optional(
            'eightd_has_key_dispenser', record)
        self.unpack_rental_methods(record)
        self.set_tstmps()

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
    region_name = Column(Text)  # how to not null?
    row_insert_tstmp = Column(DateTime)
    row_update_tstmp = Column(DateTime)
    load_id = Column(Integer, ForeignKey('load_metadata.load_id'))

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
        self.set_tstmps()


class Load_Metadata(Base):
    ''' Control table used to capture metadata of a load.
        Instantiated with dataset name, the rest is set throughout
        the load process.
        One-to-Many relationship with dimensions'''

    __tablename__ = 'load_metadata'

    load_id = Column(Integer, primary_key=True, unique=True)
    dataset = Column(Text)
    start_tstmp = Column(DateTime)
    end_tstmp = Column(DateTime)
    inserts = Column(Integer)
    updates = Column(Integer)

    regions = relationship("System_Region",
                           order_by=System_Region.region_id,
                           back_populates='load')

    stations = relationship("Station_Information",
                            order_by=Station_Information.station_id,
                            back_populates="load")

    def __init__(self, dataset):
        ''' a new metadata record should be
            instantiated with the name of the dataset
            start time will be inserted automatically '''
        self.dataset = dataset
        self.start_tstmp = datetime.now()
        self.end_tstmp = None
        self.inserts = None
        self.updates = None

    def __repr__(self):
        return ('<Load_Metadata(\n'
                f'load_id={self.load_id}\n'
                f'dataset={self.dataset}\n'
                f'start_tstmp={self.start_tstmp}\n'
                f'end_tstmp={self.end_tstmp}\n'
                f'inserts={self.inserts}\n'
                f'updates={self.updates}\n'
                ')>')
