import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import pymssql

env_path = Path(__file__).resolve().parent.parent / ".env"

#load_dotenv ensure os.getenv() can read the values from the .env file
load_dotenv()

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
# ---------- TABLE NAMES ----------
# Switch to the real names at cutover. Nothing else changes.
PIPELINE_INSTANCE        = "pipeline_instance_dummy"
LOST_VISIBILITY_LOG      = "lost_visibility_log_dummy"
ACTIVE_COMPANIES         = "active_companies_dummy"
VISIBILITY_LOSS_ANALYSIS = "visibility_loss_analysis_dummy"
TBL_VISIBILITY           = "tbl_visibility_dummy"
ECO_DRIVING              = "eco_driving_dummy"

# Read-only (never written) - point at production
REPORT_SETTINGS      = "report_settings"
DEVICE_VEHICLE_MAP   = "device_vehicle_map"
COMPANY_VEHICLES_NEW = "company_vehicles_new"
DEVICES              = "devices"
VEHICLE_ZONES        = "vehicle_zones"

def prepare_pipeline_start_end():
    truncate_sql = f"TRUNCATE TABLE {PIPELINE_INSTANCE};"

    select_sql = f"""
        SELECT
            CONCAT(DATEADD(day, -1, CAST(GETDATE() AS DATE)), ' ', LEFT(start_time, 8)) AS start_date,
            CONCAT(DATEADD(day, -1, CAST(GETDATE() AS DATE)), ' ', LEFT(end_time, 8)) AS end_date,
            IIF(DATENAME(DW, DATEADD(day, -1, CAST(GETDATE() AS DATE))) = 'Sunday', 1, 0) AS is_sunday
            FROM {REPORT_SETTINGS};
    """

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(truncate_sql)
        cursor.execute(select_sql)
        row = cursor.fetchone()
        connection.commit()
        cursor.close()

        if row is None:
            raise ValueError (f"{REPORT_SETTINGS} returned no rows.") # 2 factor authentication

        params = {
        "start_date": str(row[0]),
        "end_date": str(row[1]),
        "is_sunday": int(row[2]),
        }
        print(f"Window: {params['start_date']} -> {params['end_date']} (is_sunday = {params['is_sunday']})")
        return params

    except Exception as e:
        connection.rollback()
        print(f"Error in prepare_pipeline_start_end: {e}")
        raise
    finally:
        connection.close()

def insert_new_pipeline_details(start_date, end_date, is_sunday):
    insert_vu_sql =f"""
        INSERT INTO {PIPELINE_INSTANCE} (report_type, start_date, end_date, is_sunday)
        VALUES ('vehicle_utilization', %s, %s, %s);
    """

    insert_gs_sql = f"""
        INSERT INTO {PIPELINE_INSTANCE}
            (report_type, start_date, end_date, is_sunday, modelled_status, ingested_status)
        VALUES ('general_summary', %s, %s, %s, 1, 1);
    """

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(insert_vu_sql, (start_date, end_date, is_sunday))
        cursor.execute(insert_gs_sql, (start_date, end_date, is_sunday))
        connection.commit()
        print(f"Inserted the control rows into {PIPELINE_INSTANCE}.")
        cursor.close()
    
    except Exception as e:
        connection.rollback()
        print(f"Error in insert_new_pipeline_details: {e}")
        raise
    finally:
        connection.close()

def run_container_1():
    params = prepare_pipeline_start_end()
    insert_new_pipeline_details(
        params["start_date"],
        params["end_date"],
        params["is_sunday"],
    )
    return params

def get_fvl_parameters():
    """SSIS: Container 2 > 'Get pipeline parameters'.
    Reads the general_summary row where visibility=0. Returns id + date window."""

    select_sql = f"""
        SELECT TOP 1 id, start_date, end_date
        FROM {PIPELINE_INSTANCE}
        WHERE report_type = 'general_summary' AND visibility = 0
        ORDER BY id DESC;
    """

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()

        if row is None:
            raise ValueError(f"No general_summary row with visibility=0 in {PIPELINE_INSTANCE}.")

        params = {
            "fvl_pipeline_id": int(row[0]),
            "fvl_pipeline_start_date": str(row[1]),
            "fvl_pipeline_end_date": str(row[2]),
        }
        print(f"FVL params: id={params['fvl_pipeline_id']}, "
              f"{params['fvl_pipeline_start_date']} -> {params['fvl_pipeline_end_date']}")
        return params

    except Exception as e:
        print(f"Error in get_fvl_parameters: {e}")
        raise
    finally:
        connection.close()

def move_fvl_data(start_date, end_date):
    """SSIS: Container 2 > 'Move data'.
    Extract from Postgres (tc_events + tc_positions), filtered by the date window,
    then append into lost_visibility_log. Returns row count inserted."""

    # Extract: explicit columns in INSERT order (not e.*), so positions line up.
    extract_sql = """
        SELECT
            e.id,
            e.type,
            e.eventtime,
            e.deviceid,
            e.positionid,
            e.geofenceid,
            e.attributes,
            e.maintenanceid,
            p.latitude,
            p.longitude,
            p.address
        FROM tc_events AS e
        LEFT JOIN tc_positions AS p ON e.positionid = p.id
        WHERE e.eventtime >= %s AND e.eventtime <= %s
    """

    # Load: 11 columns, same order. eventtime->servertime, latitude->event_latitude,
    # longitude->event_longitude are renames (handled by position).
    insert_sql = f"""
        INSERT INTO {LOST_VISIBILITY_LOG}
            (id, type, servertime, deviceid, positionid, geofenceid,
             attributes, maintenanceid, event_latitude, event_longitude, Address)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    pg_conn = get_postgres_connection()
    try:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(extract_sql, (start_date, end_date))   # psycopg2 uses %s
        rows = pg_cursor.fetchall()
        pg_cursor.close()
        print(f"Extracted {len(rows)} rows from Postgres tc_events.")
    finally:
        pg_conn.close()

    if not rows:
        print("No rows in the date window; nothing to load.")
        return 0

    mssql_conn = get_mssql_connection()
    try:
        cursor = mssql_conn.cursor()
        cursor.executemany(insert_sql, rows)                     # APPEND (no truncate)
        mssql_conn.commit()
        print(f"Inserted {len(rows)} rows into {LOST_VISIBILITY_LOG}.")
        cursor.close()
    except Exception as e:
        mssql_conn.rollback()
        print(f"Error loading {LOST_VISIBILITY_LOG}: {e}")
        raise
    finally:
        mssql_conn.close()

    return len(rows)

def mark_fvl_successful(fvl_pipeline_id):
    """SSIS: Container 2 > 'Mark as successful'.
    UPDATE pipeline_instance SET visibility=1 for this run's id."""

    update_sql = f"UPDATE {PIPELINE_INSTANCE} SET visibility = 1 WHERE id = %s;"

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (fvl_pipeline_id,))
        connection.commit()
        print(f"Marked visibility=1 for id={fvl_pipeline_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_fvl_successful: {e}")
        raise
    finally:
        connection.close()

def run_container_2():
    params = get_fvl_parameters()
    move_fvl_data(params["fvl_pipeline_start_date"], params["fvl_pipeline_end_date"])
    mark_fvl_successful(params["fvl_pipeline_id"])
    return params

ANALYSIS_INSERT_SQL = f"""
WITH resolve_lost_visibility_ranking AS(
SELECT ROW_NUMBER()over(PARTITION BY a.deviceid ORDER BY a.deviceid,a.servertime) as 'rank',a.id,a.type,a.servertime,b.deviceid,a.exclude_yn
FROM (
    SELECT *
    FROM {LOST_VISIBILITY_LOG}
    WHERE type in ('deviceUnknown', 'deviceOnline')
    and FORMAT(convert(datetime,servertime),'yyyy-MM-dd ') = FORMAT(convert(datetime,%(start_date)s),'yyyy-MM-dd ')
) a
right JOIN (
    SELECT g.* FROM {DEVICE_VEHICLE_MAP} g
    INNER JOIN {COMPANY_VEHICLES_NEW} j ON j.vehicle_id = g.vehicleid
    WHERE company_id = %(company_id)s
        AND deviceid IN (SELECT a.id as item from  {DEVICES} a
                        INNER JOIN {DEVICE_VEHICLE_MAP} b
                            on a.id = b.deviceid)
)b ON a.deviceid = b.deviceid
)
,
resolve_last_status_for_all_vehicles AS(
SELECT a.*,b.deviceid as device
FROM (
    SELECT a.id,a.type,a.deviceid,mx_time FROM {LOST_VISIBILITY_LOG} a
    INNER JOIN (
        SELECT deviceid , max(servertime) as 'mx_time' FROM
        {LOST_VISIBILITY_LOG}
        WHERE servertime < convert(datetime,%(start_date)s)
        and type in ('deviceUnknown', 'deviceOnline')
        GROUP BY deviceid) b
        on a.deviceid = b.deviceid AND a.servertime = b.mx_time
    INNER JOIN {DEVICE_VEHICLE_MAP} g
        on g.deviceid = b.deviceid
    INNER JOIN {COMPANY_VEHICLES_NEW} j
        ON j.vehicle_id = g.vehicleid
    WHERE company_id = %(company_id)s and g.deviceid in(SELECT a.id as item
                                                                from  {DEVICES} a
                                                                INNER JOIN {DEVICE_VEHICLE_MAP} b
                                                                    on a.id = b.deviceid )
    ) a RIGHT JOIN(
        SELECT deviceid FROM  {DEVICE_VEHICLE_MAP} g
        INNER JOIN {COMPANY_VEHICLES_NEW} j ON j.vehicle_id = g.vehicleid
        WHERE company_id = %(company_id)s and deviceid in(SELECT a.id as item
                                            from  {DEVICES} a
                                            INNER JOIN {DEVICE_VEHICLE_MAP} b
                                                on a.id = b.deviceid )
    )b on b.deviceid = a.deviceid
)
,
resolve_lost_status_for_associated_vehicles  as(
SELECT
c.deviceid,c.servertime,c.id,c.rank,
iif((max_rank = c.rank and c.type= 'deviceUnknown'),datediff(ss,c.servertime,%(end_date)s),0) as 'time_diff_3',
time_diff,time_diff_2,max_rank
FROM (
    SELECT a.*,b.type as 'prev_status',b.servertime as 'prev_servertime',
    iif(a.rank = 1 and a.type= 'deviceOnline',datediff(ss,%(start_date)s,a.servertime),0) as time_diff_2,
    iif(a.type = 'deviceOnline' and b.type = 'deviceUnknown',datediff(ss,b.servertime,a.servertime),0) as time_diff
        FROM
        (
            SELECT * FROM resolve_lost_visibility_ranking
        ) a LEFT JOIN(
            SELECT * FROM resolve_lost_visibility_ranking
        ) b ON a.deviceid = b.deviceid and a.rank = (b.rank + 1)
        INNER JOIN {DEVICE_VEHICLE_MAP} g ON g.deviceid = a.deviceid
        INNER JOIN {COMPANY_VEHICLES_NEW} j ON j.vehicle_id = g.vehicleid
        WHERE company_id = %(company_id)s  and g.deviceid in(SELECT a.id as item
                                                from  {DEVICES} a
                                                INNER JOIN {DEVICE_VEHICLE_MAP} b
                                                    on a.id = b.deviceid)
) c left join (
    SELECT deviceid,max(rank) as max_rank from (
        SELECT * FROM resolve_lost_visibility_ranking
    )a group by deviceid
) d on d.deviceid = c.deviceid
                )
,
FINAL_RESULT AS (
                SELECT company_id,convert(DATETIME,%(start_date)s) as report_date,xDevice as deviceid,zone,offline_duration FROM(
                SELECT company_id,zone,xDevice,sum(offline) as 'offline_duration' FROM(
            SELECT company_id,zone, xDevice,sum(lwr_bdry+mid_bdry+new_upr_bdry+prev_lost_time) as 'offline'
            FROM(
                SELECT * ,iif(rank=1,iif(first_status = 'deviceOnline' and lst_status = 'deviceUnknown',upr_bdry,0),0) as new_upr_bdry from
                (
                    SELECT b.servertime,first_id,first_status,b.id as 'validate_first_id',b.rank,c.type as 'prev_days_status',b.deviceid,c.device as 'xDevice',time_diff_3 as 'lwr_bdry',time_diff as 'mid_bdry',time_diff_2 as 'upr_bdry',c.type as lst_status,
                    iif(b.id is null and c.type = 'deviceUnknown',86400,0 ) as 'prev_lost_time'
                    FROM(
                        SELECT
                        device,type
                        FROM resolve_last_status_for_all_vehicles
                    )c left JOIN (
                        SELECT
                        servertime,rank,deviceid,time_diff,time_diff_2,time_diff_3,id
                        FROM resolve_lost_status_for_associated_vehicles
                    ) b on c.device = b.deviceid
                    LEFT JOIN(
                        SELECT n.id as 'first_id',n.type as 'first_status' FROM (
                            SELECT deviceid,min(rank) as min_rank from (
                                SELECT * FROM resolve_lost_visibility_ranking
                            )a group by deviceid
                        ) m INNER JOIN (
                            SELECT * FROM resolve_lost_visibility_ranking
                        ) n ON m.deviceid = n.deviceid AND m.min_rank = n.rank
                )e ON e.first_id = b.id
                ) d
                )f INNER JOIN
                {DEVICE_VEHICLE_MAP} g ON g.deviceid = f.xDevice
                INNER JOIN {COMPANY_VEHICLES_NEW} j ON j.vehicle_id = g.vehicleid
                WHERE company_id = %(company_id)s  and g.deviceid in(SELECT a.id as item
                                                            from  {DEVICES} a
                                                            INNER JOIN {DEVICE_VEHICLE_MAP} b
                                                                on a.id = b.deviceid)
                GROUP BY company_id,zone,xDevice
        )g GROUP BY company_id,zone,xDevice
        ) b
    )
,
power_cut_details AS(
    SELECT a.*,iif(b.cnt > 0,1,0) as power_cut FROM FINAL_RESULT
    a LEFT JOIN(
            SELECT ev.deviceid,count(*) as cnt
            FROM {LOST_VISIBILITY_LOG} ev
             INNER JOIN {DEVICE_VEHICLE_MAP} b ON ev.deviceid = b.deviceid
             INNER JOIN  {COMPANY_VEHICLES_NEW} c on c.vehicle_id = b.vehicleid
             INNER JOIN {DEVICES} d on d.id = b.deviceid
             WHERE ev.type IN ('alarm') AND JSON_VALUE(ev.attributes,'$.alarm') IN ('powerCut')
             AND c.company_id = %(company_id)s
             AND ev.servertime BETWEEN %(start_date)s
                AND %(end_date)s
             GROUP BY ev.deviceid
             having count(*) > 0
    )b ON a.deviceid = b.deviceid)

INSERT INTO {VISIBILITY_LOSS_ANALYSIS}
    (company_id, report_date, deviceid, zone, offline_duration, power_cut)

SELECT p.*
FROM power_cut_details AS p;
"""

def get_vla_parameters():
    """SSIS: 'Fetch pipeline details'. general_summary row where visibility_check=0."""
    select_sql = f"""
        SELECT TOP 1 id, start_date, end_date
        FROM {PIPELINE_INSTANCE}
        WHERE visibility_check = 0 AND report_type = 'general_summary'
        ORDER BY id ASC;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            raise ValueError(f"No general_summary row with visibility_check=0 in {PIPELINE_INSTANCE}.")
        params = {
            "vla_pipeline_id": int(row[0]),
            "vla_start_date": str(row[1]),
            "vla_end_date": str(row[2]),
        }
        print(f"VLA params: id={params['vla_pipeline_id']}, "
              f"{params['vla_start_date']} -> {params['vla_end_date']}")
        return params
    except Exception as e:
        print(f"Error in get_vla_parameters: {e}")
        raise
    finally:
        connection.close()

def run_visibility_loss_analysis_loop(vla_start_date, vla_end_date):
    """SSIS: For Loop Container. Loop once per unprocessed company: analyse -> append -> mark."""
    get_count_sql = f"""
        SELECT COUNT(1)
        FROM {ACTIVE_COMPANIES}
        WHERE company_status = 1 AND processed = 0;
    """
    get_company_sql = f"""
        SELECT TOP 1 company_id
        FROM {ACTIVE_COMPANIES}
        WHERE company_status = 1 AND processed = 0;
    """
    mark_processed_sql = f"UPDATE {ACTIVE_COMPANIES} SET processed = 1 WHERE company_id = %s";

    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(get_count_sql)
        total = cursor.fetchone()[0]
        print(f"Active companies to process: {total}")

        processed = 0
        while True:
            cursor.execute(get_company_sql)
            row = cursor.fetchone()
            if row is None:
                break
            current_company_id = int(row[0])

            cursor.execute(ANALYSIS_INSERT_SQL, {
                "start_date": vla_start_date,
                "end_date": vla_end_date,
                "company_id": current_company_id,
            })
            cursor.execute(mark_processed_sql, (current_company_id,))
            connection.commit()

            processed += 1
            print(f"  [{processed}/{total}] company {current_company_id} done.")

        cursor.close()
        print(f"Loop complete: {processed} companies processed.")
    except Exception as e:
        connection.rollback()
        print(f"Error in visibility loss analysis loop: {e}")
        raise
    finally:
        connection.close()

def reset_processed_status():
    """SSIS: 'Reset Processed Status' (after loop). All processed back to 0."""
    update_sql = f"UPDATE {ACTIVE_COMPANIES} SET processed = 0;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql)
        connection.commit()
        print(f"Reset all processed=0 in {ACTIVE_COMPANIES}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in reset_processed_status: {e}")
        raise
    finally:
        connection.close()

def mark_vla_successful(vla_pipeline_id):
    """SSIS: 'Mark as successful'. visibility_check=1 for the container."""
    update_sql = f"UPDATE {PIPELINE_INSTANCE} SET visibility_check = 1 WHERE id = %s;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (vla_pipeline_id,))
        connection.commit()
        print(f"Marked visibility_check=1 for id={vla_pipeline_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_vla_successful: {e}")
        raise
    finally:
        connection.close()

def run_container_3():
    params = get_vla_parameters()
    run_visibility_loss_analysis_loop(params["vla_start_date"], params["vla_end_date"])
    reset_processed_status()
    mark_vla_successful(params["vla_pipeline_id"])
    return params

# ============================================================
# CONTAINER 4 : TBL_VISIBILITY & SAFETY REPORT (warehouse-internal)
# ============================================================

LOAD_TBL_VISIBILITY_SQL = f"""
INSERT INTO {TBL_VISIBILITY}
    (cnt, report_date, registration_num, zone, sub_zone, vehicle_type,
     visibility, visibility_loss, offline_duration, year, month, day,
     week_num, count, is_sunday, week_desc, company_id)
SELECT cnt, report_date, registration_num, CAST(zone AS VARCHAR(100)) AS Zone, CAST(sub_zone AS VARCHAR(100)) AS sub_zone, vehicle_type,visibility, visibility_loss, offline_duration, year, month, day, week_num,
COALESCE(instances,0) AS count,
IIF(DATEPART(w,report_date) = 1,'SUNDAYS','OTHER DAYS') as is_sunday,
CONCAT(LEFT(DATENAME(mm,report_date),3),'- wk:',week_num) as week_desc,company_id
FROM(
      select
      Rank()OVER(ORDER BY registration_num) AS cnt,
      d.report_date,a.deviceid,c.registration_num,c.zone,c.sub_zone,e.vehicle_type,ROUND(((86400-offline_duration)/cast(86400 as float))*100,3,1) as visibility,
      ROUND(100-(((86400-offline_duration)/cast(86400 as float))*100),3,2) as visibility_loss,offline_duration,
      datepart(year,report_date) as year,datepart(month,report_date) as month,datepart(day,report_date) as day,
      (datepart(week,report_date) - datepart(week,DATEADD(DAY,1,EOMONTH(report_date,-1)))) + 1 as week_num,d.company_id
      FROM {DEVICE_VEHICLE_MAP} a
      INNER JOIN {DEVICES} b ON a.deviceid = b.id
      INNER JOIN {COMPANY_VEHICLES_NEW} c ON c.vehicle_id = a.vehicleid
      INNER JOIN {VISIBILITY_LOSS_ANALYSIS} d ON d.company_id = c.company_id and d.deviceid = a.deviceid
      LEFT JOIN {VEHICLE_ZONES} e ON e.vehicle_id = a.vehicleid
)a LEFT JOIN (
    SELECT deviceid,COUNT(*) as instances FROM(
        select
        d.report_date,a.deviceid,c.registration_num,c.zone,ROUND(((86400-offline_duration)/cast(86400 as float))*100,3,1) as visibility,
        ROUND(100-(((86400-offline_duration)/cast(86400 as float))*100),3,2) as visibility_loss,power_cut,d.company_id
        FROM {DEVICE_VEHICLE_MAP} a
        INNER JOIN {DEVICES} b ON a.deviceid = b.id
        INNER JOIN {COMPANY_VEHICLES_NEW} c ON c.vehicle_id = a.vehicleid
        INNER JOIN {VISIBILITY_LOSS_ANALYSIS} d ON d.company_id = c.company_id and d.deviceid = a.deviceid
        LEFT JOIN {VEHICLE_ZONES} e ON e.vehicle_id = a.vehicleid
    )a WHERE visibility_loss = 100
    GROUP BY deviceid
    HAVING COUNT(*) >= 2
)b ON a.deviceid = b.deviceid
where report_date > (SELECT MAX(report_date) FROM {TBL_VISIBILITY});
"""

LOAD_ECO_DRIVING_SQL = f"""
WITH safety_report AS (
SELECT [id]
      ,[type]
      ,[servertime]
      ,[deviceid]
      ,[exclude_yn]
      ,[positionid]
      ,[geofenceid]
      ,[attributes]
      ,[maintenanceid]
      ,[event_latitude]
      ,[event_longitude]
      ,[Address]
      ,[Incremental_Value],
JSON_VALUE(attributes, '$.alarm') AS alarm_type,
CAST(servertime AS DATE) AS report_date
FROM {LOST_VISIBILITY_LOG}
WHERE type = 'alarm')
INSERT INTO {ECO_DRIVING}
    (id, Type, servertime, deviceid, positionid, geofenceid,
     alarm_type, latitude, longitude, Address, report_date)
SELECT id, type, servertime, deviceid, positionid, geofenceid, alarm_type, event_latitude, event_longitude, [address], report_date
FROM safety_report
WHERE alarm_type IN ('hardAcceleration', 'hardBraking', 'hardCornering')
    AND report_date > (SELECT MAX(report_date) FROM {ECO_DRIVING});
"""

def get_container_4_id():
    """Container 4 needs the general_summary id. visibility_check is already 1 by now,
    so we fetch by report_type only (no flag filter)."""
    select_sql = f"""
        SELECT TOP 1 id
        FROM {PIPELINE_INSTANCE}
        WHERE report_type = 'general_summary'
        ORDER BY id ASC;
    """
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(select_sql)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            raise ValueError(f"No general_summary row in {PIPELINE_INSTANCE}.")
        return int(row[0])
    except Exception as e:
        print(f"Error in get_container_4_id: {e}")
        raise
    finally:
        connection.close()

def load_tbl_visibility():
    """SSIS: Container 4 > 'Load dump table'. Watermark-driven insert into tbl_visibility."""
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(LOAD_TBL_VISIBILITY_SQL)
        connection.commit()
        print(f"Loaded {cursor.rowcount} rows into {TBL_VISIBILITY}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in load_tbl_visibility: {e}")
        raise
    finally:
        connection.close()

def load_eco_driving():
    """SSIS: Container 4 > 'Safety Report'. Watermark-driven insert into eco_driving."""
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(LOAD_ECO_DRIVING_SQL)
        connection.commit()
        print(f"Loaded {cursor.rowcount} rows into {ECO_DRIVING}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in load_eco_driving: {e}")
        raise
    finally:
        connection.close()

def mark_eco_driving_successful(pipeline_id):
    """SSIS: Container 4 > 'Mark as successful'. eco_driving_etl=1."""
    update_sql = f"UPDATE {PIPELINE_INSTANCE} SET eco_driving_etl = 1 WHERE id = %s;"
    connection = get_mssql_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(update_sql, (pipeline_id,))
        connection.commit()
        print(f"Marked eco_driving_etl=1 for id={pipeline_id}.")
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error in mark_eco_driving_successful: {e}")
        raise
    finally:
        connection.close()

def run_container_4():
    pipeline_id = get_container_4_id()
    load_tbl_visibility()
    load_eco_driving()
    mark_eco_driving_successful(pipeline_id)
    return pipeline_id

if __name__ == "__main__":   
    run_container_1()
    run_container_2()
    run_container_3()
    run_container_4()