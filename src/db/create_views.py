'''
create views for latest non-deleted data
'''
import sqlite3

create_system_regions_view = '''CREATE VIEW v_system_regions AS
                            SELECT
                            last_updated,
                            region_id,
                            name,
                            region_md5
                            FROM system_regions
                            WHERE latest_row_ind = 'Y'
                            AND transtype != 'D'
                            '''

create_station_information_view = '''CREATE VIEW v_station_information AS
                                SELECT
                                last_updated,
                                station_id,
                                short_name,
                                name,
                                lat,
                                lon,
                                capacity,
                                region_id,
                                eightd_has_key_dispenser,
                                rental_method_KEY,
                                rental_method_CREDITCARD,
                                rental_method_PAYPASS,
                                rental_method_APPLEPAY,
                                rental_method_ANDROIDPAY,
                                rental_method_TRANSITCARD,
                                rental_method_ACCOUNTNUMBER,
                                rental_method_PHONE,
                                station_md5
                                FROM station_information
                                WHERE latest_row_ind = 'Y'
                                AND transtype != 'D' '''

create_station_status_view = '''CREATE VIEW v_station_status AS
                                SELECT
                                latest.last_updated,
                                latest.station_id,
                                station_status.num_bikes_available,
                                station_status.num_bikes_disabled,
                                station_status.num_docks_available,
                                station_status.num_docks_disabled,
                                station_status.is_installed,
                                station_status.is_renting,
                                station_status.is_returning,
                                station_status.last_reported
                                FROM (
                                    SELECT MAX(last_updated) AS
                                        last_updated,station_id
                                    FROM station_status
                                    GROUP BY station_id) latest
                                INNER JOIN station_status
                                ON  latest.last_updated =
                                    station_status.last_updated
                                AND latest.station_id =
                                    station_status.station_id '''


def main():
    connection = sqlite3.connect('bikeshare.db')
#   connection.execute('DROP VIEW v_station_information')
#   connection.execute('DROP VIEW v_system_regions')
    connection.execute('DROP VIEW v_station_status')
#   connection.execute(create_system_regions_view)
#   connection.execute(create_station_information_view)
    connection.execute(create_station_status_view)
    connection.commit()
    connection.close()

if __name__ == '__main__':
    main()
