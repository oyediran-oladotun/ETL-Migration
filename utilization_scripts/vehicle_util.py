import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2                      # PostgreSQL - source (tc_positions)
import pymssql                       # SQL Server - warehouse
import pyodbc


env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

required = [
    "SDB_HOST", "SDB_PORT", "SDB_USER", "SDB_PASSWORD", "SDB_NAME",
    "DEST_HOST", "DEST_PORT", "DEST_USER", "DEST_PASSWORD", "DEST_DATABASE",
]
missing = [v for v in required if not os.getenv(v)]
if missing:
    raise RuntimeError(f".env not loaded or missing keys: {missing} (looked in {env_path})")

def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("SDB_HOST"),
        port=int(os.getenv("SDB_PORT")),
        user=os.getenv("SDB_USER"),
        password=os.getenv("SDB_PASSWORD"),
        dbname=os.getenv("SDB_NAME"),
    )

def get_mssql_connection():
    return pymssql.connect(
        server=os.getenv("DEST_HOST"),
        port=os.getenv("DEST_PORT"),
        user=os.getenv("DEST_USER"),
        password=os.getenv("DEST_PASSWORD"),
        database=os.getenv("DEST_DATABASE"),
    )

def get_mssql_odbc_connection():
    """ODBC connection used ONLY for the high-volume Container 1 load.
    pyodbc + fast_executemany batches rows into a single parameterised call,
    which is far faster than pymssql's executemany AND — unlike pymssql's
    bulk_copy — encodes nvarchar correctly, so the attributes JSON survives."""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.getenv('DEST_HOST')},{os.getenv('DEST_PORT')};"
        f"DATABASE={os.getenv('DEST_DATABASE')};"
        f"UID={os.getenv('DEST_USER')};"
        f"PWD={os.getenv('DEST_PASSWORD')};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

VU_POSITION_TEMP_3       = "vehicle_util_position_data_temp_3_dummy"
VU_POSITION_TEMP         = "vehicle_util_position_data_temp_dummy"
VU_ANALYTICS_RESULT_2    = "vu_analytics_result_2_dummy"
LOST_VISIBILITY_LOG      = "lost_visibility_log_dummy" 
NIGHT_DRIVING_ANALYTICS  = "night_driving_analytics_dummy"
PIPELINE_INSTANCE        = "pipeline_instance_dummy"
ACTIVE_COMPANIES         = "active_companies_dummy"
COMPANY_PREFERENCES      = "company_preferences_dummy"
PARKING_REPORT_ANALYTICS = "parking_report_analytics_dummy"
VU_ANALYTICS_PROC        = "prc_vu_analytics_result_2_dummy"
TBL_4HOUR_VIOLATION      = "tbl_4hour_violation_dummy"
ANALYTICAL_DATASET_SUMMARY = "analytics_dataset_summary_dummy"
ANALYTICAL_SUMMARY_PROC    = "prc_analytical_dataset_summary_dummy"
GENERAL_SUMMARY_ANALYTICS = "general_summary_analytics_dummy"
GENERAL_SUMMARY_PROC      = "general_summary_query_dummy"
NEW_PARKING_PROC          = "new_parking_query_dummy"
DEVICES                   = "devices_dummy"
DEVICE_VEHICLE_MAP        = "device_vehicle_map_dummy"
COMPANY_VEHICLES_NEW      = "company_vehicles_new_dummy"

def drop_indexes():
    """SSIS Package 1: 'Uninstall Indexes'.
    Drops five nonclustered indexes before the main ETL, so large inserts run
    without index-maintenance overhead. Each drop is guarded by IF EXISTS,
    exactly as in SSIS, so a missing index does not error."""

    drop_sql = f"""
        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID('{VU_POSITION_TEMP_3}')
                     AND name = 'ncx_backtracking_search')
        BEGIN
            DROP INDEX ncx_backtracking_search ON {VU_POSITION_TEMP_3};
        END;

        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID('{VU_POSITION_TEMP}')
                     AND name = 'ncx_backtracking_search')
        BEGIN
            DROP INDEX ncx_backtracking_search ON {VU_POSITION_TEMP};
        END;

        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID('{VU_ANALYTICS_RESULT_2}')
                     AND name = 'ncx_search_analytics_result')
        BEGIN
            DROP INDEX ncx_search_analytics_result ON {VU_ANALYTICS_RESULT_2};
        END;

        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID('{LOST_VISIBILITY_LOG}')
                     AND name = 'ncx_visibility_log')
        BEGIN
            DROP INDEX ncx_visibility_log ON {LOST_VISIBILITY_LOG};
        END;

        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID('{NIGHT_DRIVING_ANALYTICS}')
                     AND name = 'ncx_search_night_driving_model')
        BEGIN
            DROP INDEX ncx_search_night_driving_model ON {NIGHT_DRIVING_ANALYTICS};
        END;
    """

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(drop_sql)          # all five drops in one batch
        connection.commit()
        print("Dropped the five nonclustered indexes.")
        cursor.close()
    except Exception as e:
        connection.rollback()   # rolls back the connection operation incase of error
        print(f"Error in drop_indexes: {e}")
        raise
    finally:
        connection.close()

def run_package_1():
    """SSIS Package 1: Drop Indexes."""
    drop_indexes()

def get_ingestion_parameters():
    """SSIS: Container 1 > 'Fetch pipeline with ingestion need'.
    Reads the vehicle_utilization row where ingested_status=0. Returns id + date window."""

    select_sql = f"""
        SELECT TOP 1 id, start_date, end_date
        FROM {PIPELINE_INSTANCE}
        WHERE ingested_status = 0 AND report_type = 'vehicle_utilization'
        ORDER BY id DESC;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        if row is None:             # 2 FA to check if pipeline instance is not updated
            raise ValueError(f"No vehicle_utilization row with ingested_status=0 in {PIPELINE_INSTANCE}.")
        params = {
            "vehicle_util_ingestion_id": int(row[0]),
            "vehicle_util_ingestion_start_date": str(row[1]),
            "vehicle_util_ingestion_end_date": str(row[2]),
        }
        print(f"Ingestion params: id={params['vehicle_util_ingestion_id']}, "
              f"{params['vehicle_util_ingestion_start_date']} -> {params['vehicle_util_ingestion_end_date']}")
        return params
    except Exception as e:
        print(f"Error in get_ingestion_parameters: {e}")
        raise
    finally:
        connection.close()

def fetch_position_data(pipeline_id, start_date, end_date):
    """SSIS: Container 1 > 'Bring in yesterday's data'.
    Extract from tc_positions (Postgres) by fixtime window, append into temp_3 (warehouse).
    Returns row count inserted."""
    # Extract: 17 columns as SSIS selects them. pipeline_id is a literal we pass in,
    # not a source column, so it's bound as a parameter (first %s).
    # NOTE: altitude IS selected here (faithful to SSIS) but is NOT loaded — the SSIS
    # data flow read it from source and dropped it. See load below.
    extract_sql = """
        SELECT
            a.id,
            a.deviceid,
            a.servertime,
            a.speed,
            a.attributes,
            a.protocol,
            a.devicetime,
            a.fixtime,
            a.valid,
            a.latitude,
            a.longitude,
            a.altitude,
            a.course,
            a.address,
            a.accuracy,
            a.network
        FROM tc_positions a
        WHERE a.fixtime >= %s AND a.fixtime <= %s;
    """
    # Load: 16 columns into temp_3. pipeline_id is prepended per row (the SSIS
    # 'CAST(@pipeline_id AS INT) as pipeline_id' first column). altitude is omitted
    # because temp_3 has no altitude column — the data flow dropped it.
    # servertime -> server_time is a rename, handled by position.
   
    # pyodbc uses ? placeholders, not %s
    insert_sql = f"""
        INSERT INTO {VU_POSITION_TEMP_3}
            (pipeline_id, id, deviceid, server_time, speed, attributes, protocol,
             devicetime, fixtime, valid, latitude, longitude, course, address,
             accuracy, network)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
    # insert_sql = f"""
    #     INSERT INTO {VU_POSITION_TEMP_3}
    #         (pipeline_id, id, deviceid, server_time, speed, attributes, protocol,
    #          devicetime, fixtime, valid, latitude, longitude, course, address,
    #          accuracy, network)
    #     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    # """
    pg_conn = get_postgres_connection()
    try:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute("SET statement_timeout = 0;")
        pg_cursor.execute(extract_sql, (start_date, end_date))
        rows = pg_cursor.fetchall()
        pg_cursor.close()
        print(f"Extracted {len(rows)} rows from Postgres tc_positions.")
    finally:
        pg_conn.close()

    if not rows:
        print("No rows in the date window; nothing to load.")
        return 0

    # Rebuild each row for the insert: prepend pipeline_id, drop altitude (index 11).
    # Source order:  0.id 1.deviceid 2.servertime 3.speed 4.attributes 5.protocol
    #                6.devicetime 7.fixtime 8.valid 9.latitude 10.longitude
    #                11.altitude 12.course 13.address 14.accuracy 15.network
    load_rows = [
        (pipeline_id,
         r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10],
         r[12], r[13], r[14], r[15])      # note: r[11] (altitude) skipped
        for r in rows
    ]

    mssql_conn = get_mssql_connection()
    try:
        cursor = mssql_conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {VU_POSITION_TEMP_3};")   # clear the corrupt rows
        mssql_conn.commit()

        batch_size = 50000
        total = len(load_rows)
        for i in range(0, total, batch_size):
            cursor.executemany(insert_sql, load_rows[i:i + batch_size])
            mssql_conn.commit()
            print(f"  Inserted {min(i + batch_size, total)}/{total} rows...")
        print(f"Inserted {total} rows into {VU_POSITION_TEMP_3}.")
        cursor.close()
    except Exception as e:
        try:
            mssql_conn.rollback()
        except Exception:
            pass
        print(f"Error loading {VU_POSITION_TEMP_3}: {e}")
        raise
    finally:
        try:
            mssql_conn.close()
        except Exception:
            pass
    return len(load_rows)

"""
    mssql_conn = get_mssql_odbc_connection()
    try:
        cursor = mssql_conn.cursor()
        cursor.fast_executemany = True        # <-- THE setting that makes this fast
        cursor.execute(f"TRUNCATE TABLE {VU_POSITION_TEMP_3};")
        mssql_conn.commit()

        batch_size = 50000
        total = len(load_rows)
        for i in range(0, total, batch_size):
            cursor.executemany(insert_sql, load_rows[i:i + batch_size])
            mssql_conn.commit()
            print(f"  Inserted {min(i + batch_size, total)}/{total} rows...")
        print(f"Inserted {total} rows into {VU_POSITION_TEMP_3}.")
        cursor.close()
    except Exception as e:
        try: mssql_conn.rollback()
        except Exception: pass
        print(f"Error loading {VU_POSITION_TEMP_3}: {e}")
        raise
    finally:
        try: mssql_conn.close()
        except Exception: pass

    return len(load_rows)
"""

def mark_ingested(pipeline_id):
    """SSIS: Container 1 > 'Mark ingested'.
    Sets ingested_status=1 for this run's pipeline row."""

    update_sql = f"UPDATE {PIPELINE_INSTANCE} SET ingested_status = 1 WHERE id = %s;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (pipeline_id,))
        connection.commit()
        print(f"Marked ingested_status=1 for id={pipeline_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_ingested: {e}")
        raise
    finally:
        connection.close()

def run_container_1():
    params = get_ingestion_parameters()
    fetch_position_data(
        params["vehicle_util_ingestion_id"],
        params["vehicle_util_ingestion_start_date"],
        params["vehicle_util_ingestion_end_date"],
    )
    mark_ingested(params["vehicle_util_ingestion_id"])
    return params

# Bring in previous data container
def get_prev_partition_number():
    """SSIS: Container 2 > 'Fetch previous partition number'.
    temp_3 now holds today's UTC partition. We need the PREVIOUS one (partition_number - 1)
    to complete the Nigerian day. Returns that previous partition number."""

    select_sql = f"""
        WITH pn AS (
            SELECT DISTINCT p.partition_number
            FROM sys.partitions p
            INNER JOIN sys.tables t ON p.object_id = t.object_id
            WHERE t.name = '{VU_POSITION_TEMP_3}'
              AND p.rows > 0
        )
        SELECT partition_number - 1 AS prev_partition_number
        FROM pn;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            raise ValueError(
                f"No populated partition found in {VU_POSITION_TEMP_3}. "
                "Container 1 must load data before Container 2 can find its partition."
            )
        prev_partition = int(row[0])
        print(f"Previous partition number: {prev_partition}")
        return prev_partition
    except Exception as e:
        print(f"Error in get_prev_partition_number: {e}")
        raise
    finally:
        connection.close()

def switch_partition(prev_partition_number):
    """SSIS: Container 2 > 'Switch partitions'.
    Moves the previous day's partition from the archive table (temp) into the
    working table (temp_3) — a metadata-only operation, instant regardless of size.
    After this, temp_3 holds today's partition PLUS yesterday's, enough to carve
    out a complete Nigerian day."""

    # Both tables share PS_daily_data_management_2, so the switch is valid.
    # The partition number is CAST to INT exactly as SSIS did.
    switch_sql = f"""
        ALTER TABLE {VU_POSITION_TEMP}
        SWITCH PARTITION {int(prev_partition_number)}
        TO {VU_POSITION_TEMP_3}
        PARTITION {int(prev_partition_number)};
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(switch_sql)
        connection.commit()
        print(f"Switched partition {prev_partition_number} from {VU_POSITION_TEMP} "
              f"into {VU_POSITION_TEMP_3}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in switch_partition: {e}")
        raise
    finally:
        connection.close()

def run_container_2():
    prev_partition = get_prev_partition_number()
    switch_partition(prev_partition)
    return prev_partition

def get_analytics_parameters():
    """SSIS: Container 3 > 'Get pipeline parameters'.
    Reads the row where modelled_status=0 AND completed_status=0. Returns id + dates."""
    select_sql = f"""
        SELECT TOP 1 id AS pipeline_id,
            CONCAT(CAST(start_date AS DATE), ' ', LEFT(CAST(start_date AS TIME), 8)) AS vu_analytics_start_date,
            CONCAT(CAST(end_date AS DATE), ' ', LEFT(CAST(end_date AS TIME), 8)) AS vu_analytics_end_date
        FROM {PIPELINE_INSTANCE}
        WHERE modelled_status = 0 AND completed_status = 0
        ORDER BY id ASC;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            raise ValueError(f"No row with modelled_status=0 AND completed_status=0 in {PIPELINE_INSTANCE}.")
        params = {
            "vu_analytics_pipeline_id": int(row[0]),
            "vu_analytics_start_date": str(row[1]),
            "vu_analytics_end_date": str(row[2]),
        }
        print(f"Analytics params: id={params['vu_analytics_pipeline_id']}, "
              f"{params['vu_analytics_start_date']} -> {params['vu_analytics_end_date']}")
        return params
    except Exception as e:
        print(f"Error in get_analytics_parameters: {e}")
        raise
    finally:
        connection.close()

def get_active_company_count():
    """SSIS: 'Get active companies'."""
    select_sql = f"SELECT COUNT(1) FROM {ACTIVE_COMPANIES} WHERE company_status = 1;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        total = cursor.fetchone()[0]
        cursor.close()
        print(f"Active companies: {total}")
        return total
    except Exception as e:
        print(f"Error in get_active_company_count: {e}")
        raise
    finally:
        connection.close()

def get_current_company_id():
    """SSIS: 'Get current company_id'."""
    select_sql = f"""
        SELECT TOP 1 company_id
        FROM {ACTIVE_COMPANIES}
        WHERE company_status = 1 AND processed = 0;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        return int(row[0]) if row else None
    except Exception as e:
        print(f"Error in get_current_company_id: {e}")
        raise
    finally:
        connection.close()

def get_company_timezone(company_id):
    """SSIS: sub-container > 'Fetch company preference'.
    Returns the company's timezone offset (e.g. '+01:00'), defaulting to 0 if absent."""
    select_sql = f"""
        SELECT timezone FROM (
            SELECT company_id, ISNULL(attribute, 0) AS timezone
            FROM {COMPANY_PREFERENCES}
            WHERE company_id = %s AND method = 'timezone'
        ) a;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql, (company_id,))
        row = cursor.fetchone()
        cursor.close()
        timezone = str(row[0]) if row else "0"
        print(f"  company {company_id} timezone: {timezone}")
        return timezone
    except Exception as e:
        print(f"Error in get_company_timezone: {e}")
        raise
    finally:
        connection.close()

def exec_vu_analytics(company_id, timezone, start_date, end_date, pipeline_id):
    """SSIS: sub-container > 'Exec prc vu analytics'.
    Calls the stored procedure, which MERGEs into vu_analytics_result_2(_dummy).
    The proc is untouched — Python only calls it. Its write target lives in the
    proc body, NOT in our constants, so the dummy proc must target dummy tables."""
    exec_sql = f"""
        EXEC {VU_ANALYTICS_PROC}
            @company_id = %(company_id)s,
            @timezone_offset = %(timezone)s,
            @start_date = %(start_date)s,
            @end_date = %(end_date)s,
            @pipeline_id = %(pipeline_id)s;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(exec_sql, {
            "company_id": company_id,
            "timezone": timezone,
            "start_date": start_date,
            "end_date": end_date,
            "pipeline_id": pipeline_id,
        })
        connection.commit()
        print(f"  Ran {VU_ANALYTICS_PROC} for company {company_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in exec_vu_analytics (company {company_id}): {e}")
        raise
    finally:
        connection.close()

def mark_company_processed(company_id):
    """SSIS: 'Mark processed'."""
    update_sql = f"UPDATE {ACTIVE_COMPANIES} SET processed = 1 WHERE company_id = %s;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (company_id,))
        connection.commit()
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_company_processed: {e}")
        raise
    finally:
        connection.close()

def reset_processed_status():
    """SSIS: 'Restore processed status' (after loop)."""
    update_sql = f"UPDATE {ACTIVE_COMPANIES} SET processed = 0;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql)
        connection.commit()
        print("Reset all processed=0.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in reset_processed_status: {e}")
        raise
    finally:
        connection.close()

def mark_pipeline_complete(pipeline_id):
    """SSIS: 'Mark pipeline completion'."""
    update_sql = f"""
        UPDATE {PIPELINE_INSTANCE}
        SET modelled_status = 1, completed_status = 1
        WHERE id = %s;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (pipeline_id,))
        connection.commit()
        print(f"Marked modelled_status=1, completed_status=1 for id={pipeline_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_pipeline_complete: {e}")
        raise
    finally:
        connection.close()

def run_container_3():
    params = get_analytics_parameters()
    total = get_active_company_count()
    print(f"Processing {total} active companies...")

    processed = 0
    while True:
        company_id = get_current_company_id()
        if company_id is None:
            break

        timezone = get_company_timezone(company_id)          # Fetch company preferences

        exec_vu_analytics(                                    # Exec prc_vu_analytics
            company_id, timezone,
            params["vu_analytics_start_date"],
            params["vu_analytics_end_date"],
            params["vu_analytics_pipeline_id"],
        )

        mark_company_processed(company_id)                   # Mark processed company
        processed += 1
        print(f"  [{processed}] company {company_id} done.")

    reset_processed_status()                                 # Restore Processed
    mark_pipeline_complete(params["vu_analytics_pipeline_id"])  # Mark Pipeline Completion
    print(f"Container 3 complete: {processed} companies processed.")
    return params

#Fatigue Report
LOAD_FATIGUE_SQL = f"""
WITH group_identifiers AS (
    SELECT
        deviceid,
        server_time,
        device_status,
        time_diff,
        thedate,
        SUM(CASE WHEN device_status = 'parked' AND time_diff >= 1800 THEN 1 ELSE 0 END) OVER (
            PARTITION BY deviceid
            ORDER BY server_time
            ROWS UNBOUNDED PRECEDING
        ) AS group_id
    FROM {VU_ANALYTICS_RESULT_2}
    WHERE thedate = %(start_date)s
),
running_totals AS (
    SELECT
        deviceid,
        server_time,
        device_status,
        time_diff,
        thedate,
        SUM(CASE WHEN device_status = 'running' THEN time_diff ELSE 0 END) OVER (
            PARTITION BY deviceid, thedate, group_id
            ORDER BY server_time
            ROWS UNBOUNDED PRECEDING
        ) AS running_total
    FROM group_identifiers
)
INSERT INTO {TBL_4HOUR_VIOLATION} (deviceid, thedate, Violation)
SELECT
    deviceid,
    thedate,
    MAX(CASE WHEN running_total >= 14400 THEN 1 ELSE 0 END) AS Violation
FROM running_totals
GROUP BY thedate, deviceid;
"""

def get_fatigue_start_date():
    """Fetch the ingestion start_date for the vehicle_utilization pipeline row.
    Container 4's query filters on this exact date (the day being reported)."""
    select_sql = f"""
        SELECT TOP 1
            CONCAT(CAST(start_date AS DATE), ' ', LEFT(CAST(start_date AS TIME), 8)) AS start_date
        FROM {PIPELINE_INSTANCE}
        WHERE report_type = 'vehicle_utilization'
        ORDER BY id DESC;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            raise ValueError(f"No vehicle_utilization row in {PIPELINE_INSTANCE}.")
        return str(row[0])
    except Exception as e:
        print(f"Error in get_fatigue_start_date: {e}")
        raise
    finally:
        connection.close()

def insert_fatigue_report(start_date):
    """SSIS: Container 4 > '4-hour Violation'.
    Detects vehicles that ran 4+ cumulative hours (14400s) without a proper break,
    and appends one row per device per day into tbl_4hour_violation.
    Reads vu_analytics_result_2 (populated by the stored proc in Container 3)."""
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(LOAD_FATIGUE_SQL, {"start_date": start_date})
        connection.commit()
        print(f"Loaded {cursor.rowcount} rows into {TBL_4HOUR_VIOLATION}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in insert_fatigue_report: {e}")
        raise
    finally:
        connection.close()

def run_container_4():
    start_date = get_fatigue_start_date()
    insert_fatigue_report(start_date)
    return start_date

def get_summary_parameters():
    """SSIS: 'Fetch date params'.
    Reads the general_summary row where summary_etl=0. Returns id, effective
    start/end, and the year/month/day path values."""
    select_sql = f"""
        SELECT TOP 1
            id,
            CONCAT(CAST(start_date AS DATE), ' ', LEFT(CAST(start_date AS TIME), 8)) AS effective_start,
            CONCAT(CAST(end_date AS DATE), ' ', LEFT(CAST(end_date AS TIME), 8))     AS effective_end,
            YEAR(CAST(start_date AS DATE))  AS year_path,
            MONTH(CAST(start_date AS DATE)) AS month_path,
            DAY(CAST(start_date AS DATE))   AS day_path
        FROM {PIPELINE_INSTANCE}
        WHERE summary_etl = 0 AND report_type = 'general_summary'
        ORDER BY id DESC;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            raise ValueError(f"No general_summary row with summary_etl=0 in {PIPELINE_INSTANCE}.")
        params = {
            "pipeline_id": int(row[0]),
            "effective_start": str(row[1]),
            "effective_end": str(row[2]),
            "year_path": int(row[3]),
            "month_path": int(row[4]),
            "day_path": int(row[5]),
        }
        print(f"Summary params: id={params['pipeline_id']}, "
              f"{params['effective_start']} -> {params['effective_end']}")
        return params
    except Exception as e:
        print(f"Error in get_summary_parameters: {e}")
        raise
    finally:
        connection.close()

def exec_analytical_summary(company_id, start_date, end_date, timezone):
    """SSIS: 'EXEC prc_analytical_dataset_summary'.
    Refreshes one company's dashboard summary. The proc is untouched — Python
    only calls it; its MERGE/INSERT target lives in the proc body."""
    exec_sql = f"""
        EXEC {ANALYTICAL_SUMMARY_PROC}
            @company_id = %(company_id)s,
            @start_date = %(start_date)s,
            @end_date = %(end_date)s,
            @timezone_offset = %(timezone)s;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(exec_sql, {
            "company_id": company_id,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone,
        })
        connection.commit()
        print(f"  Ran {ANALYTICAL_SUMMARY_PROC} for company {company_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in exec_analytical_summary (company {company_id}): {e}")
        raise
    finally:
        connection.close()

def mark_summary_complete(pipeline_id):
    """SSIS: 'Mark as processed'. Sets summary_etl=1 for the pipeline row."""
    update_sql = f"UPDATE {PIPELINE_INSTANCE} SET summary_etl = 1 WHERE id = %s;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (pipeline_id,))
        connection.commit()
        print(f"Marked summary_etl=1 for id={pipeline_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_summary_complete: {e}")
        raise
    finally:
        connection.close()
    
def run_summary_package():
    params = get_summary_parameters()
    total = get_active_company_count()
    print(f"Processing {total} active companies for summary dashboard...")

    processed = 0
    while True:
        company_id = get_current_company_id()
        if company_id is None:
            break
        timezone = get_company_timezone(company_id)          # assumption #1
        exec_analytical_summary(company_id, params["effective_start"],
                                params["effective_end"], timezone)
        mark_company_processed(company_id)
        processed += 1
        print(f"  [{processed}] company {company_id} done.")

    reset_processed_status()
    mark_summary_complete(params["pipeline_id"])
    print(f"Summary package complete: {processed} companies processed.")
    return params

def drop_general_summary_indexes():
    """SSIS: General Summary > 'Drop Indexes'.
    Drops three indexes on general_summary_analytics before the load, so the
    proc's MERGE runs without index-maintenance overhead. Guarded by IF EXISTS,
    so a missing index does not error."""
    drop_sql = f"""
        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID('{GENERAL_SUMMARY_ANALYTICS}')
                     AND name = 'idx_company_id')
        BEGIN
            DROP INDEX idx_company_id ON {GENERAL_SUMMARY_ANALYTICS};
        END;

        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID('{GENERAL_SUMMARY_ANALYTICS}')
                     AND name = 'idx_the_date')
        BEGIN
            DROP INDEX idx_the_date ON {GENERAL_SUMMARY_ANALYTICS};
        END;

        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID('{GENERAL_SUMMARY_ANALYTICS}')
                     AND name = 'idx_device_id')
        BEGIN
            DROP INDEX idx_device_id ON {GENERAL_SUMMARY_ANALYTICS};
        END;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(drop_sql)
        connection.commit()
        print("Dropped the three general_summary indexes.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in drop_general_summary_indexes: {e}")
        raise
    finally:
        connection.close()

def get_general_summary_parameters():
    """SSIS: General Summary > 'Fetch Pipeline parameters'.
    Reads the general_summary row where completed_status=0. Returns id + dates."""
    select_sql = f"""
        SELECT TOP 1 id,
            CONCAT(CAST(start_date AS DATE), ' ', LEFT(CAST(start_date AS TIME), 8)) AS start_date,
            CONCAT(CAST(end_date AS DATE), ' ', LEFT(CAST(end_date AS TIME), 8)) AS end_date
        FROM {PIPELINE_INSTANCE}
        WHERE report_type = 'general_summary' AND completed_status = 0
        ORDER BY id ASC;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            raise ValueError(f"No general_summary row with completed_status=0 in {PIPELINE_INSTANCE}.")
        params = {
            "general_summary_pipeline_id": int(row[0]),
            "general_summary_pipeline_start_date": str(row[1]),
            "general_summary_pipeline_end_date": str(row[2]),
        }
        print(f"General summary params: id={params['general_summary_pipeline_id']}, "
              f"{params['general_summary_pipeline_start_date']} -> {params['general_summary_pipeline_end_date']}")
        return params
    except Exception as e:
        print(f"Error in get_general_summary_parameters: {e}")
        raise
    finally:
        connection.close()

def get_gs_current_company_id():
    """SSIS: General Summary loop > 'Get Current Company ID'.
    Uses the general_summary_processed flag (NOT the plain 'processed' column that
    Vehicle Utilization uses)."""
    select_sql = f"""
        SELECT TOP 1 company_id
        FROM {ACTIVE_COMPANIES}
        WHERE company_status = 1 AND general_summary_processed = 0;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        return int(row[0]) if row else None
    except Exception as e:
        print(f"Error in get_gs_current_company_id: {e}")
        raise
    finally:
        connection.close()
    
def get_gs_company_preferences(company_id):
    """SSIS: General Summary loop > 'Fetch company preferences'.
    Returns BOTH timezone and night_hrs_range (self-join on company_preferences
    for method='timezone' and method='night_hours')."""
    select_sql = f"""
        SELECT timezone, night_hrs_range
        FROM (
            SELECT company_id, ISNULL(attribute, 0) AS timezone
            FROM {COMPANY_PREFERENCES}
            WHERE company_id = %s AND method = 'timezone'
        ) a
        INNER JOIN (
            SELECT company_id, ISNULL(attribute, 0) AS night_hrs_range
            FROM {COMPANY_PREFERENCES}
            WHERE company_id = %s AND method = 'night_hours'
        ) b ON a.company_id = b.company_id;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql, (company_id, company_id))
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            # No matching preferences — default both, faithful to ISNULL(...,0)
            print(f"  company {company_id}: no preferences found, defaulting.")
            return {"timezone": "0", "night_hrs_range": "0"}
        prefs = {"timezone": str(row[0]), "night_hrs_range": str(row[1])}
        print(f"  company {company_id} timezone: {prefs['timezone']}, "
              f"night_hrs: {prefs['night_hrs_range']}")
        return prefs
    except Exception as e:
        print(f"Error in get_gs_company_preferences: {e}")
        raise
    finally:
        connection.close()

def exec_general_summary(company_id, timezone, start_date, end_date, pipeline_id):
    """SSIS: General Summary loop > 'Exec Stored Procedures'.
    Calls general_summary_query, which MERGEs into general_summary_analytics.
    NOTE: night_hrs_range is fetched by the preference step but the SSIS EXEC does
    NOT pass it — so we don't either. Faithful to the package. The proc is untouched."""
    exec_sql = f"""
        EXEC {GENERAL_SUMMARY_PROC}
            @company_id = %(company_id)s,
            @timezone_offset = %(timezone)s,
            @start_date = %(start_date)s,
            @end_date = %(end_date)s,
            @pipeline_id = %(pipeline_id)s;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(exec_sql, {
            "company_id": company_id,
            "timezone": timezone,
            "start_date": start_date,
            "end_date": end_date,
            "pipeline_id": pipeline_id,
        })
        connection.commit()
        print(f"  Ran {GENERAL_SUMMARY_PROC} for company {company_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in exec_general_summary (company {company_id}): {e}")
        raise
    finally:
        connection.close()

def mark_gs_company_processed(company_id):
    """SSIS: 'Mark processed company'. general_summary_processed=1."""
    update_sql = f"UPDATE {ACTIVE_COMPANIES} SET general_summary_processed = 1 WHERE company_id = %s;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (company_id,))
        connection.commit()
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_gs_company_processed: {e}")
        raise
    finally:
        connection.close()


def reset_gs_processed_status():
    """SSIS: 'Restore Processed'. general_summary_processed=0 for all."""
    update_sql = f"UPDATE {ACTIVE_COMPANIES} SET general_summary_processed = 0;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql)
        connection.commit()
        print("Reset all general_summary_processed=0.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in reset_gs_processed_status: {e}")
        raise
    finally:
        connection.close()


def mark_gs_pipeline_complete(pipeline_id):
    """SSIS: 'Mark Pipeline Completion'. completed_status=1."""
    update_sql = f"UPDATE {PIPELINE_INSTANCE} SET completed_status = 1 WHERE id = %s;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (pipeline_id,))
        connection.commit()
        print(f"Marked completed_status=1 for id={pipeline_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_gs_pipeline_complete: {e}")
        raise
    finally:
        connection.close()

def run_general_summary_package():
    drop_general_summary_indexes()                       # Drop Indexes
    params = get_general_summary_parameters()            # Fetch Pipeline parameters
    total = get_active_company_count()                   # Get Active companies
    print(f"Processing {total} active companies for general summary...")

    processed = 0
    while True:
        company_id = get_gs_current_company_id()         # Get Current Company ID
        if company_id is None:
            break

        prefs = get_gs_company_preferences(company_id)   # Fetch company preferences

        exec_general_summary(                            # Exec Stored Procedures
            company_id,
            prefs["timezone"],
            params["general_summary_pipeline_start_date"],
            params["general_summary_pipeline_end_date"],
            params["general_summary_pipeline_id"],
        )

        mark_gs_company_processed(company_id)            # Mark processed company
        processed += 1
        print(f"  [{processed}] company {company_id} done.")

    reset_gs_processed_status()                          # Restore Processed
    mark_gs_pipeline_complete(params["general_summary_pipeline_id"])  # Mark Pipeline Completion
    print(f"General summary package complete: {processed} companies processed.")
    return params

def get_parking_parameters():
    """SSIS: Parking > 'Fetch parameters'.
    Reads the general_summary row where stoppage_etl=0 (this warehouse stamps the
    parking flag on the general_summary row). Returns id (=parking_id) + dates."""
    select_sql = f"""
        SELECT TOP 1 id,
            CONCAT(CAST(start_date AS DATE), ' ', LEFT(CAST(start_date AS TIME), 8)) AS parking_start_date,
            CONCAT(CAST(end_date AS DATE), ' ', LEFT(CAST(end_date AS TIME), 8)) AS parking_end_date
        FROM {PIPELINE_INSTANCE}
        WHERE report_type = 'general_summary' AND stoppage_etl = 0
        ORDER BY id ASC;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            raise ValueError(f"No general_summary row with stoppage_etl=0 in {PIPELINE_INSTANCE}.")
        params = {
            "parking_id": int(row[0]),
            "parking_start_date": str(row[1]),
            "parking_end_date": str(row[2]),
        }
        print(f"Parking params: id={params['parking_id']}, "
              f"{params['parking_start_date']} -> {params['parking_end_date']}")
        return params
    except Exception as e:
        print(f"Error in get_parking_parameters: {e}")
        raise
    finally:
        connection.close()

def get_parking_current_company_id():
    """SSIS: Parking loop > 'Get Current Company ID'. Uses parking_processed flag."""
    select_sql = f"""
        SELECT TOP 1 company_id
        FROM {ACTIVE_COMPANIES}
        WHERE company_status = 1 AND parking_processed = 0;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        return int(row[0]) if row else None
    except Exception as e:
        print(f"Error in get_parking_current_company_id: {e}")
        raise
    finally:
        connection.close()


def get_parking_company_preferences(company_id):
    """SSIS: Parking loop > 'Fetch company preferences'. timezone + night_hrs_range."""
    select_sql = f"""
        SELECT timezone, night_hrs_range
        FROM (
            SELECT company_id, ISNULL(attribute, 0) AS timezone
            FROM {COMPANY_PREFERENCES}
            WHERE company_id = %s AND method = 'timezone'
        ) a
        INNER JOIN (
            SELECT company_id, ISNULL(attribute, 0) AS night_hrs_range
            FROM {COMPANY_PREFERENCES}
            WHERE company_id = %s AND method = 'night_hours'
        ) b ON a.company_id = b.company_id;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql, (company_id, company_id))
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            print(f"  company {company_id}: no preferences found, defaulting.")
            return {"timezone": "0", "night_hrs_range": "0"}
        prefs = {"timezone": str(row[0]), "night_hrs_range": str(row[1])}
        print(f"  company {company_id} timezone: {prefs['timezone']}, night_hrs: {prefs['night_hrs_range']}")
        return prefs
    except Exception as e:
        print(f"Error in get_parking_company_preferences: {e}")
        raise
    finally:
        connection.close()

PARKING_SQL = f"""
DECLARE @start_date DATETIME = %(parking_start_date)s;
DECLARE @end_date DATETIME = %(parking_end_date)s;
DECLARE @timezone_offset VARCHAR(10) = %(timezone)s;
DECLARE @company_id INT = %(current_company_id)s;

WITH generate_series AS(
    SELECT 1 AS linked,convert(datetime,switchoffset(@start_date,
                      IIF(LEFT(@timezone_offset,1) = '-',CONCAT('+',SUBSTRING(@timezone_offset,2,99)),CONCAT('-',SUBSTRING(@timezone_offset,2,99)))
                      ))  AS generate_series
    UNION ALL
    SELECT 1 as linked,DATEADD(DAY, 1, generate_series)
    FROM generate_series
    WHERE generate_series < CONVERT(datetime,switchoffset(@end_date,
                        IIF(LEFT(@timezone_offset,1) = '-',CONCAT('+',SUBSTRING(@timezone_offset,2,99)),CONCAT('-',SUBSTRING(@timezone_offset,2,99)))
                    ))
)
,generate_hrs AS(
        SELECT 1 AS linked,convert(datetime,switchoffset(@start_date,
                      IIF(LEFT(@timezone_offset,1) = '-',CONCAT('+',SUBSTRING(@timezone_offset,2,99)),CONCAT('-',SUBSTRING(@timezone_offset,2,99)))
                      ))  AS generate_hrs
        UNION ALL
        SELECT 1 as linked,DATEADD(hour, 1, generate_hrs)
        FROM generate_hrs
        WHERE generate_hrs < CONVERT(datetime,switchoffset(@end_date,
                        IIF(LEFT(@timezone_offset,1) = '-',CONCAT('+',SUBSTRING(@timezone_offset,2,99)),CONCAT('-',SUBSTRING(@timezone_offset,2,99)))
                    ))
)
,VEHICLES_MAPPING_WITH_DATES AS (
     SELECT link,device_id,linked,generate_series as thedate,DATEADD(HOUR, CAST(LEFT(@timezone_offset, 3) AS INT) ,generate_series) AS nigerian_fix_time
     FROM(
        SELECT 1 as link, id as device_id FROM {DEVICES} WHERE id in(
                SELECT distinct b.id
                FROM {DEVICES} b
                INNER JOIN {DEVICE_VEHICLE_MAP} c on c.deviceid = b.id
                INNER JOIN {COMPANY_VEHICLES_NEW} d ON d.vehicle_id = c.vehicleid
                WHERE d.company_id =  @company_id
        )
    )a INNER JOIN generate_series b ON a.link = b.linked
)
,DATASET AS(
SELECT device_id,row_cnt as rank,thedate,nigerian_fix_time,(distances * 0.001) as distances, COALESCE(ignition,'false') AS ignition,motion,id,protocol,fixtime,latitude,longitude,(1.852 * speed) as speed,
address,start_date,end_date FROM(
SELECT a.*, ROW_NUMBER()OVER(PARTITION BY device_id ORDER BY thedate,fixtime) as row_cnt
    FROM(
SELECT * ,
        COALESCE(devices,device_id) as deviceid
       FROM(
SELECT link,deviceid as devices,device_id,a.linked,thedate,nigerian_fix_time,rank,distances,COALESCE(ignitions, 'false') as ignition,motions as motion,id,server_time,speed,attributes,total_distance,speed_km,device_status,prev_device_status,
          active_device_status,distance_km,time_diff_secs,total_distance_temp,pipeline_id,protocol,valid,latitude,longitude,accuracy,course,address,network,fixtime,devicetime,outlier_yn,fixed_date_link,this_hour,d_hour,
          device_link,date_hrly,nigerian_time,count_hrs,start_date,end_date,fixed_date,thehour
          FROM(
SELECT a.*,COALESCE(this_hour,DATEPART(hour ,thedate)) as d_hour
            FROM (
SELECT * FROM VEHICLES_MAPPING_WITH_DATES a
                     INNER JOIN (
select row_number() over(partition by a.deviceid order by a.fixtime) as rank,(
                                cast(json_value(attributes,'$.totalDistance') AS FLOAT)
                            ) as distances,json_value(attributes,'$.ignition') as ignitions,json_value(attributes,'$.motion') as motions,
                            a.*,DATEADD(HOUR, CAST(LEFT(@timezone_offset, 3) AS INT) ,fixtime) AS  fixed_date_link, DATEPART(hour,fixtime) as this_hour
                        from {VU_POSITION_TEMP_3} a
                        WHERE a.deviceid IN(SELECT device_id FROM VEHICLES_MAPPING_WITH_DATES) AND
                        fixtime >= convert(datetime,switchoffset(@start_date,
                                  IIF(LEFT(@timezone_offset,1) = '-',CONCAT('+',SUBSTRING(@timezone_offset,2,99)),CONCAT('-',SUBSTRING(@timezone_offset,2,99)))
                                  ))
                        and fixtime <= CONVERT(datetime,switchoffset(@end_date,
                                    IIF(LEFT(@timezone_offset,1) = '-',CONCAT('+',SUBSTRING(@timezone_offset,2,99)),CONCAT('-',SUBSTRING(@timezone_offset,2,99)))
                                ))
                                 ) b ON CAST(a.nigerian_fix_time AS DATE) = CAST(b.fixed_date_link AS DATE) and a.device_id = b.deviceid
                                 )a
                                 )a LEFT JOIN(
SELECT b.deviceid as device_link,a.*, datepart(hour,date_hrly) thehour
                    FROM(
SELECT a.*,
                        FIRST_VALUE(date_hrly)OVER(PARTITION BY cast(nigerian_time as date) order by nigerian_time ) as start_date,
                        DATEADD(second,86399,cast(FIRST_VALUE(date_hrly)OVER(PARTITION BY cast(nigerian_time as date) order by  nigerian_time ) as timestamp) ) as end_date,
                        cast(nigerian_time as date) as fixed_date
                        FROM(
SELECT a.*,row_number()over(order by date_hrly) as count_hrs
                            FROM(
SELECT linked,generate_hrs as date_hrly,DATEADD(HOUR, CAST(LEFT(@timezone_offset, 3) AS INT) ,generate_hrs) as nigerian_time
                                 FROM generate_hrs
                                 )a
                                 )a
                                 )a LEFT JOIN(
                             SELECT 1 as link, id as deviceid FROM {DEVICES} WHERE id in(SELECT device_id FROM VEHICLES_MAPPING_WITH_DATES)
                        )b on b.link = a.linked
                        )b ON a.nigerian_fix_time = b.fixed_date AND a.d_hour = b.thehour and a.device_id = b.device_link
                        )a  WHERE (COALESCE(thehour,99) != 99)
                        )a
                        )a
                        )
,RESOLVE_FIRST_AND_LAST_RANKS AS(
    SELECT device_id,nigerian_fix_time,min(rank) as first_point,max(rank) as last_point
    FROM DATASET
    GROUP BY device_id,nigerian_fix_time)
,MERGE_FIRST_LAST_WITH_DATASET AS(
    SELECT a.*,first_point,last_point
    FROM DATASET a
    INNER JOIN RESOLVE_FIRST_AND_LAST_RANKS b ON a.device_id = b.device_id
)
,RESOLVE_TIME_DIFF_BETWEEN_DATAPOINTS AS(
    SELECT *,
    (
            case when rank = first_point  and rank = last_point then (
                        datediff(second,start_date,fixtime)
                        +(
                            case when cast(fixtime as date) = cast(CURRENT_TIMESTAMP as date)
                            then 0
                            else
                            COALESCE(datediff(second,fixtime,end_date),0)
                            end
                        )
            )else(
                case when rank = first_point
                then datediff(second,start_date,fixtime)
                else (
                    case when rank = last_point
                    then (
                        COALESCE(DATEDIFF(second,prev_fixtime,fixtime),0)
                        +(
                            case when cast(fixtime as date) = cast(CURRENT_TIMESTAMP as date)
                            then 0
                            else
                            COALESCE(datediff(second,fixtime,end_date),0)
                            end
                        )
                    )
                    else
                        datediff(second,prev_fixtime,fixtime)
                    end
                )
                end
        )
        end
    )as time_diff
    FROM(
        SELECT *,LAG(fixtime,1,null)OVER(PARTITION BY device_id ORDER BY rank) AS prev_fixtime
        FROM MERGE_FIRST_LAST_WITH_DATASET
    )a
)
,LEVEL_ONE_DATA_POINT_CLASSIFICATION AS(
    SELECT * ,
    (
        CASE WHEN (speed > 3.7 OR ((speed >0  and speed <= 3.7) and ignition = 'true'))
        then 'running'
        else (
                (case when (speed = 0 and ignition = 'false') or((speed > 0 and speed <= 3.7) and ignition = 'false')
                 then 'parked'
                 else (
                        (case when (speed = 0 and ignition = 'true' )
                         then 'idle'
                         else 'offline'
                         end
                        )
                 )
                 end
                )
        )
        end
    ) AS device_status_lv1
    FROM RESOLVE_TIME_DIFF_BETWEEN_DATAPOINTS
)
,RESOLVE_FALSE_RUNNING_PARKED_TO_OFFLINE AS (
    select *,
    (
        CASE WHEN device_status_lv1 = 'running' and time_diff > 500
        then 'offline'
        ELSE (
            CASE WHEN device_status_lv1 = 'parked'
            then (
                case when last_point = rank and (datediff(second,fixtime,end_date)) > 5400
                then 'offline'
                else (
                    case when last_point != rank and time_diff > 5400 then 'offline' else device_status_lv1 end
                )
                end
            )
            else
            device_status_lv1
            end
        )
        END
    ) AS device_status_lv2
    from LEVEL_ONE_DATA_POINT_CLASSIFICATION
)
,RESOLVE_PREV_AND_CURRENT_STATUS AS (
    SELECT * ,
    case when (rank != first_point and rank != last_point)
    then (
                CASE WHEN prev_status = 'running'
                then 'running'
                ELSE (
                    CASE WHEN prev_status = 'idle'
                    THEN 'idle'
                    ELSE (
                        CASE WHEN prev_status = 'parked'
                        THEN 'parked'
                        ELSE (
                            case when prev_status = 'offline'
                            then 'offline'
                            else 'check'
                            end
                        )
                        END
                    )
                    END
                )
                END
    )
    else device_status_lv2
    end
     as device_status_lv3
    FROM (
        select *,
        LAG(device_status_lv2,1,null)OVER(PARTITION BY device_id ORDER BY rank) as prev_status
        from RESOLVE_FALSE_RUNNING_PARKED_TO_OFFLINE
        )a
)
, RESOLVE_OFFLINE_AT_FIRST_AND_LAST AS(
    select * ,
    (
        CASE WHEN (rank = first_point)
        then (
            case when (
                (
                    (device_status_lv3 = 'idle' or device_status_lv3 = 'running') and time_diff <= 500
                )OR(
                    (device_status_lv3 = 'parked') and time_diff <= 5400
                )
            )
            then device_status_lv3
            else 'offline'
            end
        ) else (
                CASE WHEN (rank = last_point)
                then(
                    case when (
                        (
                            (device_status_lv3 = 'idle' or device_status_lv3 = 'running') and (datediff(second ,fixtime,end_date)) > 500
                        )OR(
                            (device_status_lv3 = 'parked') and (datediff(second,fixtime,end_date)) > 5400
                        )
                    )
                    then 'offline'
                    else
                    device_status_lv3
                    end
                )else device_status_lv3
            end
        )
        end
    ) AS device_status_lv4
    from RESOLVE_PREV_AND_CURRENT_STATUS
)
,PREV_DISTANCE AS(
    SELECT
        LAG(distances,1,null)OVER(PARTITION BY device_id ORDER BY rank) as prev_distance,
        *
    FROM RESOLVE_OFFLINE_AT_FIRST_AND_LAST
),DISTANCE_DIFFERENCE AS (
    SELECT (distances - prev_distance) as dist_diff,
    *
    FROM PREV_DISTANCE
),POSSIBLE_DISTANCE AS (
    select
    (
    (time_diff*0.000277778)*120
    )
    as max_diff,*
    FROM DISTANCE_DIFFERENCE
),IDENTIFY_DRIFTS AS(
    SELECT (
        CASE WHEN dist_diff > (max_diff + 5)
        THEN  dist_diff
        ELSE  0
        END
    ) AS drift,
    *
    from POSSIBLE_DISTANCE
), ACCUMULATE_EST_DISTANCE AS (
    SELECT * FROM IDENTIFY_DRIFTS
)
,RESOLVE_PARKING_START_AND_END AS(
SELECT a.*,
    (
        CASE WHEN first_point = rank and device_status_lv4 = 'parked'
        THEN (CASE WHEN time_diff <= 5400 then start_date else fixtime end)
        else (
                (
                    case when (prev_state = 'running' and device_status_lv4 = 'parked')
                    THEN prev_fixtime
                    else fixtime
                    end
                )
        )
        end
    ) as new_start_time,
    (
        CASE WHEN last_point = rank  and device_status_lv4 = 'parked'
            THEN (CASE WHEN (datediff(second,fixtime,end_date)) <= 5400
                  then end_date
                  else next_fixtime
                  end
                 )
        else(
                next_fixtime
        )
        end
    )as new_end_time
    FROM(
    select *,
    LAG(device_status_lv4,1,null)OVER(PARTITION BY device_id ORDER BY rank) AS prev_state,
    LEAD(fixtime,1,null)OVER(PARTITION BY device_id ORDER BY rank) AS next_fixtime
    from ACCUMULATE_EST_DISTANCE
)a
)
,CREATE_CONSEQUTIVE_PARKING_GROUPS AS(
    SELECT a.*,SUM(parking_tag)OVER(PARTITION BY device_id ORDER BY rank) AS parking_group
    FROM(
        SELECT *,
        (case when device_status_lv4 = 'parked' then 0 else 1 end) as parking_tag
        FROM RESOLVE_PARKING_START_AND_END
    )a
)
SELECT *
INTO #ParkingTemp
FROM CREATE_CONSEQUTIVE_PARKING_GROUPS;

WITH prepare_parking_summary AS (
    SELECT a.device_id,a.park_start,a.park_end,b.new_start_time,c.new_end_time,
    datediff(second , b.new_start_time,c.new_end_time) AS duration,b.address,b.distances,concat(b.latitude , '.' , b.longitude) as coord,b.latitude,b.longitude,b.id as position_id
    FROM(
        select device_id,parking_group,min(rank) as park_start,max(rank) as park_end
        from #ParkingTemp  WHERE parking_tag = 0
        GROUP BY device_id,parking_group
    )a LEFT JOIN #ParkingTemp b ON a.device_id = b.device_id and a.park_start = b.rank
       LEFT JOIN #ParkingTemp c ON a.device_id = c.device_id and a.park_end = c.rank)
SELECT *
INTO #Parking_Summary_temp
FROM prepare_parking_summary;

WITH parking_data AS (
SELECT start_time,address,odometer,end_time,duration,latitude,longitude,deviceid,position_id,registration_num,company_id FROM(
        select ROW_NUMBER()OVER(ORDER BY d.registration_num,park_start) as rows,
        d.registration_num,DATEADD(HOUR, CAST(LEFT(@timezone_offset, 3) AS INT), new_start_time) as start_time,address,distances as odometer,DATEADD(HOUR, CAST(LEFT(@timezone_offset, 3) AS INT),new_end_time) as end_time,(
            CONCAT(
                    (duration / (60*60*24)),'  ' ,
                    ((duration % (60*60*24) / 3600)), 'h ' ,
                    (((duration % (60*60*24) % 3600) / 60)),'m ' ,
                    ((duration % 60)),'s'
                    )
        ) as duration,coord,latitude,longitude,company_id,b.id as deviceid,position_id from
        #Parking_Summary_temp a
        INNER JOIN {DEVICES} b ON a.device_id = b.id
        INNER JOIN {DEVICE_VEHICLE_MAP} c on c.deviceid = a.device_id
        LEFT JOIN {COMPANY_VEHICLES_NEW} d ON d.vehicle_id = c.vehicleid
        WHERE d.company_id =  @company_id
        )a)
INSERT INTO {PARKING_REPORT_ANALYTICS}
SELECT *
FROM parking_data;

DROP TABLE #ParkingTemp;
DROP TABLE #Parking_Summary_temp;
"""


def insert_parking_data(company_id, start_date, end_date, timezone):
    """SSIS: Parking loop > 'Insert parking data'.
    Runs the multi-CTE parking analysis for one company and appends to
    parking_report_analytics. Uses session temp tables, so the entire batch runs
    in ONE execute call. Reads temp_3 via JSON_VALUE(attributes) — temp_3 must
    hold clean JSON (loaded via executemany/pyodbc, NOT the corrupting bulk_copy)."""
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(PARKING_SQL, {
            "parking_start_date": start_date,
            "parking_end_date": end_date,
            "timezone": timezone,
            "current_company_id": company_id,
        })
        connection.commit()
        print(f"  Inserted parking data for company {company_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in insert_parking_data (company {company_id}): {e}")
        raise
    finally:
        connection.close()
    
def exec_new_parking_query(company_id, timezone, start_date, end_date):
    """SSIS: Parking loop > 'Exec new_parking_query'.
    Runs after the inline parking CTE. MERGEs into parking_report_analytics.
    Note: only 4 params — no pipeline_id. The proc is untouched."""
    exec_sql = f"""
        EXEC {NEW_PARKING_PROC}
            @company_id = %(company_id)s,
            @timezone_offset = %(timezone)s,
            @start_date = %(start_date)s,
            @end_date = %(end_date)s;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(exec_sql, {
            "company_id": company_id,
            "timezone": timezone,
            "start_date": start_date,
            "end_date": end_date,
        })
        connection.commit()
        print(f"  Ran {NEW_PARKING_PROC} for company {company_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in exec_new_parking_query (company {company_id}): {e}")
        raise
    finally:
        connection.close()

def mark_parking_company_processed(company_id):
    """SSIS: 'Mark processed company'. parking_processed=1."""
    update_sql = f"UPDATE {ACTIVE_COMPANIES} SET parking_processed = 1 WHERE company_id = %s;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (company_id,))
        connection.commit()
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_parking_company_processed: {e}")
        raise
    finally:
        connection.close()


def reset_parking_processed_status():
    """SSIS: 'Restore Processed'. parking_processed=0."""
    update_sql = f"UPDATE {ACTIVE_COMPANIES} SET parking_processed = 0;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql)
        connection.commit()
        print("Reset all parking_processed=0.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in reset_parking_processed_status: {e}")
        raise
    finally:
        connection.close()


def mark_parking_pipeline_complete(parking_id):
    """SSIS: 'Mark Pipeline Completion'. stoppage_etl=1."""
    update_sql = f"UPDATE {PIPELINE_INSTANCE} SET stoppage_etl = 1 WHERE id = %s;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (parking_id,))
        connection.commit()
        print(f"Marked stoppage_etl=1 for id={parking_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_parking_pipeline_complete: {e}")
        raise
    finally:
        connection.close()

def run_parking_package():
    params = get_parking_parameters()
    total = get_active_company_count()
    print(f"Processing {total} active companies for parking report...")

    processed = 0
    while True:
        company_id = get_parking_current_company_id()
        if company_id is None:
            break

        prefs = get_parking_company_preferences(company_id)

        insert_parking_data(                             # inline parking CTE
            company_id,
            params["parking_start_date"],
            params["parking_end_date"],
            prefs["timezone"],
        )
        exec_new_parking_query(                          # then the proc
            company_id,
            prefs["timezone"],
            params["parking_start_date"],
            params["parking_end_date"],
        )

        mark_parking_company_processed(company_id)
        processed += 1
        print(f"  [{processed}] company {company_id} done.")

    reset_parking_processed_status()
    mark_parking_pipeline_complete(params["parking_id"])
    print(f"Parking package complete: {processed} companies processed.")
    return params

if __name__ == "__main__":
    # run_package_1()
    # run_container_1()      # loads today's partition into temp_3
    # run_container_2()      # switches yesterday's partition in from temp
    # run_container_3()
    # run_container_4()
    run_summary_package()
    run_general_summary_package()
    run_parking_package()

    