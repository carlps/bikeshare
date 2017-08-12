'''
This script gets capital bikeshare data, validates against db, and loads

Data from https://gbfs.capitalbikeshare.com/gbfs/gbfs.json
For more details, check https://github.com/NABSA/gbfs/blob/master/gbfs.md

The gbfs.json file lists what files are available for consumption.

For now we're just going to use station info, station status and system regions
There are models for each in models.py file

SQLAlchemy is used as an ORM
'''

import requests
import os
import logging
from datetime import datetime

from sqlalchemy.orm import make_transient

from .models import Station_Status, Station_Information
from .models import System_Region, Dimension, Load_Metadata
from .utils import get_session


def get_data(model, metadata):
    ''' Lookup bikeshare data and return dict of objects like {id:obj}.
        Model should be one of the main data models in models.py
            ie System_Region or Station_Status
        Metadata should be an instance of a Load_Metadata object
        which will be updated with amount of source rows.
        Whatever model is passed is the type of objects returned. '''

    results = {}
    table_name = model.__tablename__

    # model should only be subclasses of Dimension
    if not issubclass(model, Dimension):
        raise TypeError('model should be child of Dimension')

    # build url using param
    url = f'https://gbfs.capitalbikeshare.com/gbfs/en/{table_name}.json'

    # attempt to get url
    response = requests.get(url)
    # if response is not good, raise error
    response.raise_for_status()
    # retrieve json and break into pieces needed
    response_json = response.json()

    # ensure data is as expected
    if len(response_json['data']) != 1:
        logging.basicConfig(filename='.logs/etl.get_data.log',
                            format='%(asctime)s %(message)s',
                            level=logging.DEBUG)
        logging.debug(f"Response data from {url}" +
                       "came back in an unexpected format.")
        logging.debug(f'json: {response_json}')
        logging.debug("Expected response['data'] to be a " +
                      "dictionary with one entry.")
        raise ValueError('API response came back in unexpected format')

    # get first (and only) value from data, which is list of dicts
    data = list(response_json['data'].values())[0]
    # create an object for each row pulled down
    for row in data:
        row = model(row)
        row.load_id = metadata.load_id
        results[row.id] = row
    # update metadata
    metadata.src_rows = len(results)
    print(f'{model.__name__}: extract done. {len(results)} rows of data')
    # return dictionary
    return results


def compare_data(data, model, metadata, session):
    ''' Compare data from API to current data in DB.
        If db data exists in new data but doesn't match: update.
        If db data doesn't exist in new data: insert.
        If db data exists and matches: do nothing
        If nothing in db data: insert all '''
    upd_count = 0
    # get all db objects with same id as new data
    for match in session.query(model).filter(model.id.in_(data.keys())).all():
        # if values in the row don't match, merge with new record
        # != operator overridden to compare only certain attributes
        if match != data[match.id]:
            data[match.id].transtype = 'U'
            session.merge(data.pop(match.id))
            upd_count += 1
        # if they do match, simply delete from data, since no change
        else:
            del(data[match.id])
    # everything in data at this point is new inserts only
    for insert in data:
        data[insert].transtype = 'I'
        session.add(data[insert])
    # update metadata
    metadata.updates = upd_count
    metadata.inserts = len(session.new)
    metadata.end_tstmp = datetime.now()
    session.add(metadata)
    print(f'{model.__name__}: compared data.',
          f'{metadata.inserts} inserts. ',
          f'{metadata.updates} updates.')


def etl(model, session):
    ''' Get data, transform, update old if needed, insert new'''
    metadata = Load_Metadata(model.__tablename__, session)
    data = get_data(model, metadata)
    compare_data(data, model, metadata, session)
    session.commit()
    print(f'{model.__name__} load complete.')
    print(f'inserted {metadata.inserts} and updated {metadata.updates}')
    print(f'see load_metadata table, load_id: {metadata.load_id}')


def main():
    session = get_session()
    etl(System_Region, session)
    etl(Station_Information, session)
    session.close()


if __name__ == '__main__':
    main()
