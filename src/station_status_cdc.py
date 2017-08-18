''' src.station_status_cdc '''

import json  # temp for test files
import requests
from time import time, sleep, strftime
from datetime import datetime

from sqlalchemy import and_
from sqlalchemy.sql import func

from .utils import get_session
from .models import Station_Status


def get_latest_from_db(session):
    ''' Get latest station_status data from database
        Returns dict with station_id as key and object as value '''
    # first define subquery which gets latest timestamp and ID
    subq = session.query(func.max(Station_Status.last_updated).
                         label('last_updated'),
                         Station_Status.station_id).group_by(
        Station_Status.station_id).subquery()

    # get latest by joining station_status with subquery
    latest = session.query(Station_Status).join(
        subq, and_(Station_Status.last_updated == subq.c.last_updated,
                   Station_Status.station_id == subq.c.station_id)).all()

    latest_dict = {}
    for row in latest:
        # need to expunge and make transient because
        # we just want copies of the data, not connected to db
        latest_dict[row.station_id] = row
    return latest_dict


def get_data_from_api():
    ''' Get data from api.
        Make sure to return data, last_updated, and ttl '''
    url = 'https://gbfs.capitalbikeshare.com/gbfs/en/station_status.json'
    response = requests.get(url)
    # if response is not good, raise error
    response.raise_for_status()
    return response.json()


def row_is_new(new_obj, latest_data):
    ''' Check if Station_Status is already in latest data
        If no, it's new!
        If yes, and rows are different, it should be loaded.
    '''

    if new_obj.station_id not in latest_data.keys():
        print(f'new row! id: {new_obj.station_id}')
        return True
    else:
        if (new_obj != latest_data[new_obj.station_id]):
            return True
    return False


def load_db(out, session):
    ''' Load new data into db '''
    session.add_all(out)
    session.commit()


def station_status_cdc(session):
    ''' Get latest DB data and new API data.
        Iterate through API data. Compare against
        latest. If an API row is new, add to output
        list and replace record in latest dict.
        Once all API rows have been checked,
        load output list, then sleep until the
        API should be refreshed.
    '''
    latest_data = get_latest_from_db(session)
    while True:  # loop until ^C pressed
        try:
            out = []
            new_data = get_data_from_api()
            print(f'got data for {new_data["last_updated"]} at {time():.2f}')
            for new_row in new_data['data']['stations']:
                new_row['last_updated'] = new_data['last_updated']
                new_obj = Station_Status(new_row)
                if row_is_new(new_obj, latest_data):
                    out.append(new_obj)
                    latest_data[new_obj.station_id] = new_obj
            if len(out) > 0:
                load_db(out, session)
                print(f'inserted {len(out)} rows at '
                      f'{datetime.strftime(datetime.now(),"%c")} '
                      f'for {new_data["last_updated"]}')
            else:
                print('no changes. nothing to load '
                      f'for {new_data["last_updated"]}')
            ''' So let's talk timestamps. I originally wrote
                logic to calculate sleep time based on last_update
                and ttl. This didn't guarantee new files. Testing
                with just sleeping the ttl no matter when the file
                was pulled proved to guarantee new results with
                no missed files. Based on requirements, I think
                it's preferable to potentially get an update a
                few seconds late rather than repeatedly pulling
                the same data. '''
            sleep_time = new_data['ttl']
            print(f'got data. will check again in {sleep_time}')
            sleep(sleep_time)
        except KeyboardInterrupt:
            break


def main():
    session = get_session()
    station_status_cdc(session)
    session.close()
    print('\nokay session closed')


if __name__ == '__main__':
    main()
