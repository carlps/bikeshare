CREATE OR REPLACE FUNCTION update_user_load_metadata() RETURNS trigger AS $update_user_load_metadata$
	BEGIN
		-- check is row was midified by a different user
		IF (new.modified_by != current_user) THEN
			UPDATE load_metadata
			SET modified_by = current_user
			WHERE load_id = NEW.load_id;
		END IF;
		RETURN NULL; -- result is ignored since this is an AFTER trigger
	END;
$update_user_load_metadata$ LANGUAGE plpgsql;

CREATE TRIGGER load_metadata_user
AFTER UPDATE ON load_metadata
	FOR EACH ROW EXECUTE PROCEDURE update_user_load_metadata();

CREATE OR REPLACE FUNCTION update_user_system_regions() RETURNS trigger AS $update_user_system_regions$
	BEGIN
		-- check is row was midified by a different user
		IF (new.modified_by != current_user) THEN
			UPDATE system_regions
			SET modified_by = current_user
			WHERE region_id = NEW.region_id;
		END IF;
		RETURN NULL; -- result is ignored since this is an AFTER trigger
	END;
$update_user_system_regions$ LANGUAGE plpgsql;

CREATE TRIGGER system_regions_user_update
AFTER UPDATE ON system_regions
	FOR EACH ROW EXECUTE PROCEDURE update_user_system_regions();

CREATE OR REPLACE FUNCTION update_user_station_information() RETURNS trigger AS $update_user_station_information$
	BEGIN
		-- check is row was midified by a different user
		IF (new.modified_by != current_user) THEN
			UPDATE station_information
			SET modified_by = current_user
			WHERE station_id = NEW.station_id;
		END IF;
		RETURN NULL; -- result is ignored since this is an AFTER trigger
	END;
$update_user_station_information$ LANGUAGE plpgsql;

CREATE TRIGGER station_information_user_update
AFTER UPDATE ON station_information
	FOR EACH ROW EXECUTE PROCEDURE update_user_station_information();