# --- Imports: brings in the the toolboxes this script needs
import os                             # lets us read environment variables (our .env values)
from pathlib import Path              # a clean, safe way to build file paths
from dotenv import load_dotenv        # installed through pip, helps loads the .env file so os.getenv can see our secrets

import mysql.connector                # the MySQL driver: lets Python talk to a MySQL database
from mysql.connector import Error     # the specific error type MySQL raises, so we can catch it
import psycopg2                       # the postgres driver connector
import pymssql
from pymongo import MongoClient

# configurations that allows us have access to the files inside the ref_table_migration
env_path = Path(__file__).resolve().parent.parent / ".env"

#load_dotenv ensure os.getenv() can read the values from the .env file
load_dotenv(dotenv_path = env_path)

def get_mysql_connection(): # get _mysql_connection opens the connection to Mysql
    #Open and RETURN a live MySQL connection. The caller closes it.
        return mysql.connector.connect(                    # return allows us to bring back what we declared earlier
            host=os.getenv("MYSQL_HOST"),                  # the server address       
            user=os.getenv("MYSQL_USER"),                  # the username
            password=os.getenv("MYSQL_PASSWORD"),          # the password
            database=os.getenv("MYSQL_DATABASE"),          # which database to use
            port=int(os.getenv("MYSQL_PORT")),             # the port, turned into a number
        )

#postgres script
def get_postgres_connection():
    #Open and RETURN a live PostgreSQL connection. The caller closes it.
        return psycopg2.connect(
            host = os.getenv("SDB_HOST"),
            port = int(os.getenv("SDB_PORT")),
            user = os.getenv("SDB_USER"),
            password = os.getenv("SDB_PASSWORD"),
            dbname = os.getenv("SDB_NAME"),
        )

 # mongodb connection
def get_mongo_client():
    #Open and RETURN a live MongoDB client. The caller closes it.
        return MongoClient (
            os.getenv("MONGO_URI")
            )

# mssql connection
def get_mssql_connection():
    #Open and RETURN a live MSSQL (warehouse) connection. The caller closes it.
        return pymssql.connect(
            server = os.getenv("DEST_HOST"),
            port = os.getenv("DEST_PORT"),
            user= os.getenv("DEST_USER"),
            password = os.getenv("DEST_PASSWORD"),
            database = os.getenv("DEST_DATABASE"),
        )

def create_dummy_company_vehicles_new():
    create_sql = """
    IF OBJECT_ID('company_vehicles_new_dummy', 'U') IS NULL
    CREATE TABLE company_vehicles_new_dummy (
        company_id               BIGINT         NOT NULL,
        vehicle_id               BIGINT         NOT NULL,
        registration_num         NVARCHAR(60)   NULL,
        third_party_logistic_id  BIGINT         NULL,
        vehicle_tonnage_capacity BIGINT         NULL,
        make                     NVARCHAR(510)  NULL,
        zone                     NVARCHAR(MAX)  NULL,
        sub_zone                 NVARCHAR(MAX)  NULL,
        isActive                 INT            NULL,
        record_position          BIGINT         NULL,
        vehicle_type             NVARCHAR(100)  NULL
    );
    """
    
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(create_sql)
        connection.commit()
        print("Dummy table company_vehicles_new_dummy is ready.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error creating company_vehicles_new dummy table: {e}")
    finally:
        connection.close()

def extract_company_vehicle_new():
    query = """
        WITH company_vehicles_new AS (
            SELECT CAST(company_id AS SIGNED) AS company_id, vehicle_id,
                   CASE WHEN company_id = 724
                        THEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(registration_number,'\\r',''),'\\n',''),' ',''),'-',''),'\\t',''))
                        ELSE registration_number END AS registration_number,
                   third_party_logistic_id, vehicle_tonnage_capacity, make,
                   JSON_VALUE(zone,'$.labelType') AS zone,
                   JSON_UNQUOTE(JSON_EXTRACT(zone,'$.labelSubTypes[0].labelSubType')) AS sub_zone,
                   isActive,
                   ROW_NUMBER() OVER (PARTITION BY company_id, registration_number ORDER BY deleted_at DESC) AS record_position,
                   vehicle_type_id
            FROM (
                SELECT user_id AS company_id, v.id AS vehicle_id, v.registration_number,
                       v.third_party_logistic_id, v.vehicle_tonnage_capacity, make,
                       v.deleted_at, v.zone,
                       CASE WHEN v.deleted_at IS NULL THEN 1 ELSE 0 END AS isActive,
                       v.vehicle_type_id
                FROM users u
                INNER JOIN vehicles v ON v.user_id = u.id
                WHERE u.deleted_at IS NULL
                  AND v.deleted_at IS NULL
                  AND user_id <> 18
            ) xx
        )
        SELECT c.company_id, c.vehicle_id, c.registration_number, c.third_party_logistic_id,
               c.vehicle_tonnage_capacity, c.make, c.zone, CAST(c.sub_zone AS CHAR) AS sub_zone,
               c.isActive, c.record_position, v.name AS vehicle_type
        FROM company_vehicles_new AS c
        INNER JOIN vehicle_types AS v ON c.vehicle_type_id = v.id;
    """

    connection = get_mysql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        print(f"Extracted {len(rows)} rows from MySQL company_vehicle_new (active).")
        return rows
    except Error as e:
        print(f"Error extracting company_vehicle_new data: {e}")
        return []
    finally:
        connection.close()

def flush_and_fill_company_vehicle_new(rows):
    target_table = "company_vehicles_new"   # real 'company_vehicles_new' when live
    truncate_sql = f"TRUNCATE TABLE {target_table};"
    insert_sql = f"""
        INSERT INTO {target_table}
            (company_id, vehicle_id, registration_num, third_party_logistic_id,
             vehicle_tonnage_capacity, make, zone, sub_zone, isActive,
             record_position, vehicle_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)
        cursor.executemany(insert_sql, rows)
        connection.commit()
        print(f"Flushed and filled {len(rows)} rows into {target_table}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error during company_vehicle_new flush and fill: {e}")
    finally:
        connection.close()

def run_company_vehicle_new():
    rows = extract_company_vehicle_new()
    if not rows:
        print("No rows extracted — skipping load to avoid emptying the table.")
        return
    flush_and_fill_company_vehicle_new(rows)

def verify_company_vehicle_new():
    target_table = "company_vehicles_new"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        print(f"{target_table} now holds {cursor.fetchone()[0]} rows.")
        cursor.execute(f"SELECT TOP 5 * FROM {target_table};")
        for row in cursor.fetchall():
            print(row)
        cursor.close()
    except Exception as e:
        print(f"Error verifying company_vehicle_new load: {e}")
    finally:
        connection.close()

def create_dummy_company_vehicles_inactive():
    create_sql = """
    IF OBJECT_ID('company_vehicles_inactive_dummy', 'U') IS NULL
    CREATE TABLE company_vehicles_inactive_dummy (
        company_id               BIGINT         NOT NULL,
        vehicle_id               BIGINT         NOT NULL,
        registration_num         NVARCHAR(60)   NULL,
        third_party_logistic_id  BIGINT         NULL,
        vehicle_tonnage_capacity BIGINT         NULL,
        make                     NVARCHAR(510)  NULL,
        zone                     NVARCHAR(MAX)  NULL,
        sub_zone                 NVARCHAR(MAX)  NULL,
        isActive                 INT            NULL,
        record_position          BIGINT         NULL,
        vehicle_type             NVARCHAR(100)  NULL
    );
    """
    
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(create_sql)
        connection.commit()
        print("Dummy table company_vehicles_inactive_dummy is ready.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error creating company_vehicles_inactive dummy table: {e}")
    finally:
        connection.close()

def extract_company_vehicle_inactive():
    query = """
        WITH company_vehicles_new_inactive AS (
            SELECT CAST(company_id AS SIGNED) AS company_id, vehicle_id,
                   CASE WHEN company_id = 724
                        THEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(registration_number,'\\r',''),'\\n',''),' ',''),'-',''),'\\t',''))
                        ELSE registration_number END AS registration_number,
                   third_party_logistic_id, vehicle_tonnage_capacity, make,
                   JSON_VALUE(zone,'$.labelType') AS zone,
                   JSON_UNQUOTE(JSON_EXTRACT(zone,'$.labelSubTypes[0].labelSubType')) AS sub_zone,
                   isActive,
                   ROW_NUMBER() OVER (PARTITION BY company_id, registration_number ORDER BY deleted_at DESC) AS record_position,
                   vehicle_type_id
            FROM (
                SELECT user_id AS company_id, v.id AS vehicle_id, v.registration_number,
                       v.third_party_logistic_id, v.vehicle_tonnage_capacity, make,
                       v.deleted_at, v.zone,
                       CASE WHEN v.deleted_at IS NULL THEN 1 ELSE 0 END AS isActive,
                       v.vehicle_type_id
                FROM users AS u
                INNER JOIN vehicles_duplicates_backup AS v ON v.user_id = u.id
                WHERE u.deleted_at IS NULL
                  AND user_id <> 18
            ) AS xx
        ),
        inactive AS (
            SELECT CAST(company_id AS SIGNED) AS company_id, vehicle_id,
                   CASE WHEN company_id = 724
                        THEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(registration_number,'\\r',''),'\\n',''),' ',''),'-',''),'\\t',''))
                        ELSE registration_number END AS registration_number,
                   third_party_logistic_id, vehicle_tonnage_capacity, make,
                   JSON_VALUE(zone,'$.labelType') AS zone,
                   JSON_UNQUOTE(JSON_EXTRACT(zone,'$.labelSubTypes[0].labelSubType')) AS sub_zone,
                   isActive,
                   ROW_NUMBER() OVER (PARTITION BY company_id, registration_number ORDER BY deleted_at DESC) AS record_position,
                   vehicle_type_id
            FROM (
                SELECT user_id AS company_id, v.id AS vehicle_id, v.registration_number,
                       v.third_party_logistic_id, v.vehicle_tonnage_capacity, make,
                       v.deleted_at, v.zone,
                       CASE WHEN v.deleted_at IS NULL THEN 1 ELSE 0 END AS isActive,
                       v.vehicle_type_id
                FROM users AS u
                INNER JOIN vehicles AS v ON v.user_id = u.id
                WHERE u.deleted_at IS NULL
                  AND v.deleted_at IS NOT NULL
                  AND user_id <> 18
            ) AS yy
        )
        SELECT c.company_id, c.vehicle_id, c.registration_number, c.third_party_logistic_id,
               c.vehicle_tonnage_capacity, c.make, c.zone, CAST(c.sub_zone AS CHAR) AS sub_zone,
               c.isActive, c.record_position, v.name AS vehicle_type
        FROM company_vehicles_new_inactive AS c
        INNER JOIN vehicle_types AS v ON c.vehicle_type_id = v.id
        UNION
        SELECT c.company_id, c.vehicle_id, c.registration_number, c.third_party_logistic_id,
               c.vehicle_tonnage_capacity, c.make, c.zone, CAST(c.sub_zone AS CHAR) AS sub_zone,
               c.isActive, c.record_position, v.name AS vehicle_type
        FROM inactive AS c
        INNER JOIN vehicle_types AS v ON c.vehicle_type_id = v.id;
    """

    connection = get_mysql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        print(f"Extracted {len(rows)} rows from MySQL company_vehicle (inactive).")
        return rows
    except Error as e:
        print(f"Error extracting company_vehicle_inactive data: {e}")
        return []
    finally:
        connection.close()

def flush_and_fill_company_vehicle_inactive(rows):
    """LOAD (Branch B): truncate then insert. Same column order as Branch A."""
    target_table = "company_vehicles_inactive"

    truncate_sql = f"TRUNCATE TABLE {target_table};"
    insert_sql = f"""
        INSERT INTO {target_table}
            (company_id, vehicle_id, registration_num, third_party_logistic_id,
             vehicle_tonnage_capacity, make, zone, sub_zone, isActive,
             record_position, vehicle_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)
        cursor.executemany(insert_sql, rows)
        connection.commit()
        print(f"Flushed and filled {len(rows)} rows into {target_table}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error during company_vehicle_inactive flush and fill: {e}")
    finally:
        connection.close()

def run_company_vehicle_inactive():
    rows = extract_company_vehicle_inactive()
    if not rows:
        print("No rows extracted — skipping load to avoid emptying the table.")
        return
    flush_and_fill_company_vehicle_inactive(rows)

def verify_company_vehicle_inactive():
    target_table = "company_vehicles_inactive"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        print(f"{target_table} now holds {cursor.fetchone()[0]} rows.")
        cursor.execute(f"SELECT TOP 5 * FROM {target_table};")
        for row in cursor.fetchall():
            print(row)
        cursor.close()
    except Exception as e:
        print(f"Error verifying company_vehicle_inactive load: {e}")
    finally:
        connection.close()

def create_dummy_device_vehicle_map():
    create_sql = """
    IF OBJECT_ID('device_vehicle_map_dummy', 'U') IS NULL
    CREATE TABLE device_vehicle_map_dummy (
        deviceid    BIGINT        NOT NULL,
        vehicleid   BIGINT        NOT NULL,
        reg_number  VARCHAR(30)   NOT NULL,
        updatedat   NVARCHAR(240) NULL
    );
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(create_sql)
        connection.commit()
        print("Dummy table device_vehicle_map_dummy is ready.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error creating device_vehicle_map_dummy: {e}")
    finally:
        connection.close()

def extract_device_vehicle_map():
    client = get_mongo_client()
    try:
        db = client[os.getenv("MONGO_DB")]
        collection = db["devices"]
        documents = collection.find()

        rows = []
        skipped = 0                                  # count what we drop
        for doc in documents:
            deviceid  = doc.get("deviceId")
            vehicleid = doc.get("vehicleId")

            # both ids must be present and purely numeric (target is BIGINT) — else skip
            if deviceid is None or vehicleid is None \
               or not str(deviceid).isdigit() or not str(vehicleid).isdigit():
                skipped += 1
                continue

            rows.append((
                int(deviceid),
                int(vehicleid),
                doc.get("name"),         # -> reg_number
                doc.get("updatedAt"),    # -> updatedat
            ))

        print(f"Extracted {len(rows)} documents; skipped {skipped} with missing/non-numeric ids.")
        return rows
    except Exception as e:
        print(f"Error occurred while extracting: {e}")
        return []
    finally:
        client.close()

def flush_and_fill_device_vehicle_map(rows):
    target_table = "device_vehicle_map"
    truncate_sql = f"TRUNCATE TABLE {target_table};"
    insert_sql = f"""
        INSERT INTO {target_table}
            (deviceid, vehicleid, reg_number, updatedat)
        VALUES (%s, %s, %s, %s);
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)
        cursor.executemany(insert_sql, rows)
        connection.commit()
        print(f"Flushed and filled {len(rows)} rows into {target_table}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error occurred during flush and fill: {e}")
    finally:
        connection.close()

def run_device_vehicle_map():
    rows = extract_device_vehicle_map()
    if not rows:
        print("No rows extracted")
        return
    flush_and_fill_device_vehicle_map(rows)

def verify_device_vehicle_map():
    target_table = "device_vehicle_map"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        print(f"{target_table} now holds {cursor.fetchone()[0]} rows")
        cursor.execute(f"SELECT TOP 5 * FROM {target_table};")
        for row in cursor.fetchall():
            print(row)
        cursor.close()
    except Exception as e:
        print(f"Error verifying device_vehicle_map load: {e}")
    finally:
        connection.close()

def create_dummy_point_of_interest():
    create_sql = """
    IF OBJECT_ID('point_of_interest_dummy', 'U') IS NULL
    CREATE TABLE point_of_interest_dummy (
        name        NVARCHAR(600)  NOT NULL,
        movamid     INT            NOT NULL,
        type        NVARCHAR(40)   NOT NULL,
        poiid       INT            NOT NULL,
        radius      DECIMAL(20,8)  NULL,
        longitude   DECIMAL(20,8)  NULL,
        latitude    DECIMAL(20,8)  NULL,
        created_at  DATETIME       NULL
    );
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(create_sql)
        connection.commit()
        print("Dummy table point_of_interest_dummy is ready.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error creating point_of_interest dummy table: {e}")
    finally:
        connection.close()

def extract_point_of_interest():
    """PIPELINE step (extract): pull POI documents from MongoDB and FLATTEN each
    document into a tuple matching the warehouse column order."""
    client = get_mongo_client()
    try:
        db = client[os.getenv("MONGO_DB")]        # choose the database
        collection = db["pois"] # <-- the POI collection name

        documents = collection.find()                   # all documents (like SELECT * )

        rows = []                                        # we BUILD the list of tuples ourselves
        for doc in documents:
            coord = doc.get("co-ordinate", {})           # nested object; {} fallback if missing
            poi = (
                doc.get("name"),
                doc.get("movamId"),
                doc.get("type"),
                doc.get("poiId"),
                doc.get("radius"),
                coord.get("lng"),                        # longitude
                coord.get("lat"),                        # latitude
                doc.get("createdAt"),
            )
            rows.append(poi)                             # add this flattened row to the list

        print(f"Extracted {len(rows)} documents from MongoDB point_of_interest.")
        return rows
    except Exception as e:
        print(f"Error extracting point_of_interest data: {e}")
        return []
    finally:
        client.close()

def flush_and_fill_point_of_interest(rows):
    target_table = "point_of_interest"     # switch to real 'point_of_interest' when live

    truncate_sql = f"TRUNCATE TABLE {target_table};"
    insert_sql = f"""
        INSERT INTO {target_table}
            (name, movamid, type, poiid, radius, longitude, latitude, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """  

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)
        cursor.executemany(insert_sql, rows)
        connection.commit()
        print(f"Flushed and filled {len(rows)} rows into {target_table}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error during point_of_interest flush and fill: {e}")
    finally:
        connection.close()

def run_point_of_interest():
    rows = extract_point_of_interest()
    if not rows:
        print("No rows extracted — skipping load to avoid emptying the table.")
        return
    flush_and_fill_point_of_interest(rows)

def verify_point_of_interest():
    target_table = "point_of_interest"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        print(f"{target_table} now holds {cursor.fetchone()[0]} rows.")
        cursor.execute(f"SELECT TOP 5 * FROM {target_table};")
        for row in cursor.fetchall():
            print(row)
        cursor.close()
    except Exception as e:
        print(f"Error verifying point_of_interest load: {e}")
    finally:
        connection.close()

def create_dummy_devices():
    create_sql = """
    IF OBJECT_ID('devices_dummy', 'U') IS NULL
    CREATE TABLE devices_dummy (
        id          INT            NOT NULL,
        name        NVARCHAR(MAX)  NOT NULL,
        uniqueid    NVARCHAR(MAX)  NULL,
        lastupdate  DATETIME2      NULL,
        positionid  FLOAT          NULL,
        groupid     FLOAT          NULL,
        attributes  VARCHAR(255)   NULL,
        phone       NVARCHAR(90)   NULL,
        model       VARCHAR(255)   NULL,
        contact     VARCHAR(255)   NULL,
        category    VARCHAR(255)   NULL,
        disabled    BIT            NULL
    );
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(create_sql)
        connection.commit()
        print("Dummy table devices_dummy is ready.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error creating devices dummy table: {e}")
    finally:
        connection.close()

def extract_devices():
    """PIPELINE step (extract): pull devices from POSTGRES (tc_devices).
    Columns are named explicitly, in the warehouse target's order — the source
    has many extra columns the warehouse doesn't carry, so SELECT * is wrong here."""
    query = """
        SELECT
            id,
            name,
            uniqueid,
            lastupdate,
            positionid,
            groupid,
            attributes,
            phone,
            model,
            contact,
            category,
            disabled
        FROM tc_devices;
    """
    connection = get_postgres_connection()       # Postgres source
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        print(f"Extracted {len(rows)} rows from Postgres tc_devices.")
        return rows
    except Exception as e:
        print(f"Error extracting devices data: {e}")
        return []
    finally:
        connection.close()

def flush_and_fill_devices(rows):
    target_table = "devices"               # switch to real 'devices' when live

    truncate_sql = f"TRUNCATE TABLE {target_table};"
    insert_sql = f"""
        INSERT INTO {target_table}
            (id, name, uniqueid, lastupdate, positionid, groupid,
             attributes, phone, model, contact, category, disabled)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)
        cursor.executemany(insert_sql, rows)
        connection.commit()
        print(f"Flushed and filled {len(rows)} rows into {target_table}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error during devices flush and fill: {e}")
    finally:
        connection.close()

def run_devices():
    rows = extract_devices()
    if not rows:
        print("No rows extracted — skipping load to avoid emptying the table.")
        return
    flush_and_fill_devices(rows)

def verify_devices():
    target_table = "devices"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        print(f"{target_table} now holds {cursor.fetchone()[0]} rows.")
        cursor.execute(f"SELECT TOP 5 * FROM {target_table};")
        for row in cursor.fetchall():
            print(row)
        cursor.close()
    except Exception as e:
        print(f"Error verifying devices load: {e}")
    finally:
        connection.close()

def to_int(value):
    """Mongo fields can be numbers OR strings (e.g. vehicleId '6'). Normalise to int,
    or None if missing/unconvertible — so typed SQL columns get clean numbers."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

#COMPANY INFO SCRIPT
def create_dummy_company_info():
    """ONE-TIME dev setup: dummy target mirroring the REAL warehouse structure
    (confirmed via sp_help): NOT NULL columns, sized VARCHARs, primary key on company_id."""
    create_sql = """
    IF OBJECT_ID('company_info_dummy', 'U') IS NULL
    CREATE TABLE company_info_dummy (
        company_id    BIGINT        NOT NULL,
        company_name  VARCHAR(180)  NOT NULL,
        email         VARCHAR(255)  NOT NULL,
        CONSTRAINT PK_company_info_dummy PRIMARY KEY (company_id)
    );
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(create_sql)
        connection.commit()
        print("Dummy table company_info_dummy is ready.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error creating company_info dummy table: {e}")
    finally:
        connection.close()

def extract_company_info():
    """PIPELINE step (extract): pull one row per company (id, name, merged emails)."""
    query = """
        SELECT FLOOR(a.id) AS id, a.`name`, b.email
        FROM users a
        INNER JOIN (
            SELECT MIN(id) AS id,
                   `name`,
                   GROUP_CONCAT(DISTINCT email) AS email
            FROM `users`
            WHERE role_id = 3 AND is_owner = 1
            GROUP BY `name`
        ) AS b ON a.id = b.id
        WHERE a.is_owner = 1 AND a.role_id = 3
        ORDER BY a.id;
    """
    connection = get_mysql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        print(f"Extracted {len(rows)} rows from MySQL company_info.")
        return rows
    except Error as e:
        print(f"Error extracting company_info data: {e}")
        return []
    finally:
        connection.close()

def flush_and_fill_company_info(rows):
    """PIPELINE step (load): truncate the target, then insert all rows. Truncate-first."""
    target_table = "company_info"          # switch to the real 'company_info' when live

    truncate_sql = f"TRUNCATE TABLE {target_table};"
    insert_sql = f"""
        INSERT INTO {target_table}
            (company_id, company_name, email)
        VALUES (%s, %s, %s);
    """  # three columns -> three %s, in the order the extract returns them

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)             # FLUSH
        cursor.executemany(insert_sql, rows)     # FILL
        connection.commit()                      # truncate + insert together (all-or-nothing)
        print(f"Flushed and filled {len(rows)} rows into {target_table}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error during company_info flush and fill: {e}")
    finally:
        connection.close()

def run_company_info():
    """Runs the full company_info activity: extract, then flush-and-fill."""
    rows = extract_company_info()
    if not rows:
        print("No rows extracted — skipping load to avoid emptying the table.")
        return
    flush_and_fill_company_info(rows)

def verify_company_info():
    """Read back from the warehouse to confirm the load landed."""
    target_table = "company_info"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        print(f"{target_table} now holds {cursor.fetchone()[0]} rows.")
        cursor.execute(f"SELECT TOP 5 * FROM {target_table};")
        for row in cursor.fetchall():
            print(row)
        cursor.close()
    except Exception as e:
        print(f"Error verifying company_info load: {e}")
    finally:
        connection.close()

# COMPANY_VEHICLES SCRIPT
def create_dummy_company_vehicles():
   
    create_sql = """
    IF OBJECT_ID('company_vehicles', 'U') IS NULL
    CREATE TABLE company_vehicles_dummy (
        company_id              BIGINT             NOT NULL,
        vehicle_id              BIGINT             NOT NULL,
        registration_number     VARCHAR(30)        NULL,
        third_party_logistic_id BIGINT             NULL,
        make                    VARCHAR(255)       NULL
    );
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(create_sql)            # OBJECT_ID guard makes this safe to re-run
        connection.commit()
        print("Dummy table company_vehicles_dummy is ready.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error creating company_vehicles dummy table: {e}")
    finally:
        connection.close()

def extract_company_vehicles():
    """PIPELINE step (extract): pull company_vehicles from MySQL, returning a list of tuples."""
    query = """
        SELECT
            user_id AS company_id,
            v.id AS vehicle_id,
            v.registration_number,
            v.third_party_logistic_id,
            make
        FROM users u
        INNER JOIN vehicles v ON v.user_id = u.id
        WHERE u.deleted_at IS NULL
          AND v.deleted_at IS NULL
          AND user_id <> 18;
    """
    connection = get_mysql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        print(f"Extracted {len(rows)} rows from MySQL company_vehicles.")
        return rows
    except Error as e:
        print(f"Error extracting company_vehicles data: {e}")
        return []
    finally:
        connection.close()

def flush_and_fill_company_vehicles(rows):
    target_table = "company_vehicles"     # switch to the real 'company_vehicles' when live

    truncate_sql = f"TRUNCATE TABLE {target_table};"
    insert_sql = f"""
        INSERT INTO {target_table}
            (company_id, vehicle_id, registration_num, third_party_logistic_id, make)
        VALUES (%s, %s, %s, %s, %s);
    """  # five columns -> five %s placeholders, in the same order the extract returns them

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)             # FLUSH
        cursor.executemany(insert_sql, rows)     # FILL (one batch)
        connection.commit()                      # truncate + insert commit together (all-or-nothing)
        print(f"Flushed and filled {len(rows)} rows into {target_table}.")
        cursor.close()
    except Exception as e:
        connection.rollback()                    # if insert fails, the truncate is undone too
        print(f"Error during company_vehicles flush and fill: {e}")
    finally:
        connection.close()

def run_company_vehicles():
    rows = extract_company_vehicles()
    if not rows:                                 # guard: don't truncate if extract gave nothing
        print("No rows extracted — skipping load to avoid emptying the table.")
        return
    flush_and_fill_company_vehicles(rows)

def verify_company_vehicles():
    target_table = "company_vehicles"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        print(f"{target_table} now holds {cursor.fetchone()[0]} rows.")
        cursor.execute(f"SELECT TOP 5 * FROM {target_table};")
        for row in cursor.fetchall():
            print(row)
        cursor.close()
    except Exception as e:
        print(f"Error verifying company_vehicles load: {e}")
    finally:
        connection.close()

# after getting the connections, we begin to create tables
def create_dummy_third_party_logistics():
    create_sql = """
    IF OBJECT_ID('third_party_logistics_dummy', 'U') IS NULL
    CREATE TABLE third_party_logistics_dummy (
    id                  DECIMAL(29,0) NULL,
    user_id             DECIMAL(29,0) NULL,
    zone                NTEXT         NULL,
    sub_zone            NTEXT         NULL,
    transporter_code    NTEXT         NULL,
    transporter_name    NTEXT         NULL
);
"""
    # Establish connection to the warehouse server
    connection = get_mssql_connection()             # open the connection to the warehouse
    try:
        cursor = connection.cursor()                # the cursor to execute
        cursor.execute(create_sql)                  # this executes the query
        connection.commit()                         # .commit make it permanent
        print ("Dummy table third_party_logistics created")
        cursor.close()

    except Exception as e:
        connection.rollback()                       # undo if failure occurs
        print (f"Error occured while creating 3PL logistics dummy table: {e}")
    finally:
        connection.close()

def extract_3pl():
    query = """
        SELECT id,
               user_id,
               payloads->>'$.zone.labelType'     AS zone,
               payloads->>'$.zone.labelSubTypes'  AS sub_zone,
               payloads->>'$.transporter_code'    AS transporter_code,
               payloads->>'$.transport_name'      AS transporter_name
        FROM third_party_logistics;
        """

    connection = get_mysql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        print(f"Extracted {len(rows)} rows from MySQL third_party_logistics.")
        return rows
    except Error as e:
        print(f"Error extracting 3PL data: {e}")
        return[]
    finally:
        connection.close()

def flush_and_fill_3pl(rows):
    target_table = "third_party_logistics"

    # 'Flush': empty the table before reloading. (Pre-command, exactly like SSIS.)
    truncate_sql = f"TRUNCATE TABLE {target_table};"

    # 'Fill': one INSERT with %s placeholders — one per column, in column order.
    insert_sql = f"""
        INSERT INTO {target_table}
            (id, user_id, zone, sub_zone, transporter_code, transporter_name)
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    connection = get_mssql_connection()            # open the warehouse connection
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)               # FLUSH: empty the table first
        cursor.executemany(insert_sql, rows)       # FILL: insert all rows in one batch
        connection.commit()                        # make both the truncate and insert permanent

        print(f"Flushed and filled {len(rows)} rows into {target_table}.")
        cursor.close()

    except Exception as e:
        connection.rollback()                      # undo BOTH steps if anything failed
        print(f"Error during flush and fill: {e}")
    finally:
        connection.close()                         # always close
    
def run_3pl():
    #Runs the full 3PL activity: extract from MySQL, then flush-and-fill the warehouse.
    rows = extract_3pl()                    # 'Pull 3PL Data'

    if not rows:                            # if extract failed or returned nothing...
        print("No rows extracted — skipping load to avoid emptying the table.")
        return                              # ...don't proceed to truncate+load

    flush_and_fill_3pl(rows)                # 'Flush and Fill Warehouse'

def verify_3pl():
    """Read back from the warehouse to confirm the load actually landed."""
    target_table = "third_party_logistics"

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        count = cursor.fetchone()[0]                     # the single number returned
        print(f"{target_table} now holds {count} rows.")

        cursor.execute(f"SELECT TOP 5 * FROM {target_table};")  # TOP 5 = SQL Server's 'first 5'
        for row in cursor.fetchall():
            print(row)

        cursor.close()
    except Exception as e:
        print(f"Error verifying 3PL load: {e}")
    finally:
        connection.close()

# def peek_one_poi():
#     """Print ONE raw point-of-interest document so we can see the exact field names/shape."""
#     client = get_mongo_client()
#     try:
#         db = client[os.getenv("MONGO_DB")]   # pick the database
#         collection = db["devices"]   # <-- put the POI collection name here
#         doc = collection.find_one()                # fetch a single document
#         print(doc)
#     finally:
#         client.close()
    
# def inspect_bad_vehicle_ids():
#     """Look at documents whose vehicleId is not purely numeric — so we can decide
#     whether skipping them loses real data."""
#     client = get_mongo_client()
#     try:
#         db = client[os.getenv("MONGO_DB")]
#         collection = db["devices"]

#         total = 0
#         bad = []
#         for doc in collection.find():
#             total += 1
#             vid = doc.get("vehicleId")
#             if vid is not None and not str(vid).isdigit():   # has a non-digit character
#                 bad.append({
#                     "vehicleId": vid,
#                     "name": doc.get("name"),
#                     "deviceId": doc.get("deviceId"),
#                 })

#         print(f"Total documents: {total}")
#         print(f"Non-numeric vehicleId count: {len(bad)}")
#         print("Examples:")
#         for b in bad[:15]:          # show up to 15 so we can see the pattern
#             print(" ", b)
#     finally:
#         client.close()

def create_dummy_company_drivers():
    """ONE-TIME setup: dummy target mirroring company_drivers (Images 1 & 2).
    Confirmed: company_id IS present (col 1); there is NO dob column."""
    create_sql = """
    IF OBJECT_ID('company_drivers_dummy', 'U') IS NULL
    CREATE TABLE company_drivers_dummy (
        company_id    BIGINT         NOT NULL,
        driver_id     BIGINT         NOT NULL,
        driver_name   NVARCHAR(400)  NULL,
        phone_number  NVARCHAR(40)   NULL,
        gender        NVARCHAR(20)   NULL,
        vehicle_id    BIGINT         NULL,
        email_add     NVARCHAR(360)  NULL,
        created_at    DATETIME       NULL,
        IsActive      BIT            NULL
    );
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(create_sql)
        connection.commit()
        print("Dummy table company_drivers_dummy is ready.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error creating company_drivers dummy table: {e}")
    finally:
        connection.close()

def extract_company_drivers():
    """EXTRACT: pull drivers from MySQL, SELECTing columns in the TARGET's order.
    dob is intentionally omitted (no target column); company_id leads."""
    query = """
        SELECT
            d.user_id AS company_id,
            d.id AS driver_id,
            CAST(d.name AS CHAR) AS driver_name,
            d.phone AS phone_number,
            d.gender,
            v.id AS vehicle_id,
            CAST(d.email AS CHAR) AS email_address,
            d.created_at,
            CASE WHEN d.status = 'active' THEN 1 ELSE 0 END AS IsActive
        FROM drivers AS d
        LEFT JOIN vehicles AS v ON d.id = v.driver_id;
    """
    connection = get_mysql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        print(f"Extracted {len(rows)} rows from MySQL company_drivers.")
        return rows
    except Error as e:
        print(f"Error extracting company_drivers data: {e}")
        return []
    finally:
        connection.close()

def flush_and_fill_company_drivers(rows):
    """LOAD: truncate then insert. INSERT order matches the SELECT order above."""
    target_table = "company_drivers"

    truncate_sql = f"TRUNCATE TABLE {target_table};"
    insert_sql = f"""
        INSERT INTO {target_table}
            (company_id, driver_id, driver_name, phone_number, gender,
             vehicle_id, email_add, created_at, IsActive)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)
        cursor.executemany(insert_sql, rows)
        connection.commit()
        print(f"Flushed and filled {len(rows)} rows into {target_table}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error during company_drivers flush and fill: {e}")
    finally:
        connection.close()

def run_company_drivers():
    rows = extract_company_drivers()
    if not rows:
        print("No rows extracted — skipping load to avoid emptying the table.")
        return
    flush_and_fill_company_drivers(rows)

def verify_company_drivers():
    target_table = "company_drivers"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        print(f"{target_table} now holds {cursor.fetchone()[0]} rows.")
        cursor.execute(f"SELECT TOP 5 * FROM {target_table};")
        for row in cursor.fetchall():
            print(row)
        cursor.close()
    except Exception as e:
        print(f"Error verifying company_drivers load: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    run_company_vehicle_new()         # runs both branches
    verify_company_vehicle_new()

    run_company_vehicle_inactive()
    verify_company_vehicle_inactive()

    run_device_vehicle_map()
    verify_device_vehicle_map()

    run_point_of_interest()
    verify_point_of_interest()

    run_devices()
    verify_devices()

    run_company_info()
    verify_company_info()

    run_company_vehicles()
    verify_company_vehicles()

    run_3pl()
    verify_3pl()

    run_company_drivers()
    verify_company_drivers()
    

