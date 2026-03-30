import pandas as pd
import pyodbc
import json
from datetime import date, datetime, timedelta
import logging
import os
import config

# ============================================
# SETUP LOGGING
# ============================================
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/scd2_run.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# DATABASE CONNECTION
# ============================================
def get_conn():
    return pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=localhost\\SQLExpress;'
        'DATABASE=SCD_Project;'
        'Trusted_Connection=yes;'
    )

# ============================================
# STEP 1: LOAD STAGING DATA
# ============================================
def load_to_staging(conn, filepath):
    """Load CSV into staging table (always truncate first)"""
    logger.info(f"Loading staging data from: {filepath}")

    cursor = conn.cursor()
    df = pd.read_csv(filepath)

    # BUG FIX 2: CSV has 'source_guide' column, rename to 'source_date'
    if 'source_guide' in df.columns:
        df.rename(columns={'source_guide': 'source_date'}, inplace=True)

    # BUG FIX 3: Convert date to string — pyodbc ODBC driver 
    # cannot bind Python date objects directly (SQLBindParameter error)
    df['source_date'] = pd.to_datetime(df['source_date']).dt.strftime('%Y-%m-%d')

    # BUG FIX 4: TRUNCATE was missing — without this, re-runs 
    # stack duplicate staging records
    cursor.execute("TRUNCATE TABLE stg_customer")

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stg_customer 
            (customer_id, first_name, last_name, email, 
             city, state, customer_segment, source_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        int(row['customer_id']),
        str(row['first_name']),
        str(row['last_name']),
        str(row['email']),
        str(row['city']),
        str(row['state']),
        str(row['customer_segment']),
        str(row['source_date']))   # BUG FIX 3: passed as string

    conn.commit()
    logger.info(f"Loaded {len(df)} records into staging")
    return len(df)


# ============================================
# STEP 2: FETCH CURRENT DIMENSION STATE
# ============================================
def get_current_dimension(conn):
    """Get all CURRENTLY ACTIVE records from dim_customer"""
    query = """
        SELECT customer_sk, customer_id, first_name, last_name,
               email, city, state, customer_segment,
               effective_date, expiry_date, is_current
        FROM dim_customer
        WHERE is_current = 1
    """
    df = pd.read_sql(query, conn)
    return df.set_index('customer_id')


# ============================================
# STEP 3: FETCH STAGING DATA
# ============================================
def get_staging_data(conn):
    """Get all records from staging"""
    df = pd.read_sql("SELECT * FROM stg_customer", conn)
    return df.set_index('customer_id')


# ============================================
# STEP 4: COMPARE AND DETECT CHANGES
# ============================================
def detect_changes(current_dim, staging):
    """
    Classify each incoming record as:
    - NEW        : customer_id not in dimension at all
    - SCD2_CHANGE: a tracked column changed → preserve history
    - SOFT_UPDATE : only non-tracked columns changed → update in place
    - NO_CHANGE  : identical → skip
    """
    new_records = []
    scd2_changes = []
    soft_updates = []
    no_changes = []

    for cust_id, stg_row in staging.iterrows():

        if cust_id not in current_dim.index:
            new_records.append(cust_id)
            logger.debug(f"NEW customer: {cust_id}")
            continue

        dim_row = current_dim.loc[cust_id]

        # Check SCD2-tracked columns
        scd2_changed = False
        changed_cols = []

        for col in config.SCD2_COLUMNS:
            if str(stg_row[col]).strip() != str(dim_row[col]).strip():
                scd2_changed = True
                changed_cols.append(col)

        if scd2_changed:
            scd2_changes.append((cust_id, changed_cols))
            logger.debug(f"SCD2 CHANGE: customer {cust_id} | columns: {changed_cols}")
            continue

        # Check non-tracked columns
        soft_changed = any(
            str(stg_row[col]).strip() != str(dim_row[col]).strip()
            for col in config.NON_TRACKED_COLUMNS
        )

        if soft_changed:
            soft_updates.append(cust_id)
        else:
            no_changes.append(cust_id)

    logger.info(
        f"Change Detection → "
        f"NEW: {len(new_records)} | "
        f"SCD2: {len(scd2_changes)} | "
        f"SOFT: {len(soft_updates)} | "
        f"NO CHANGE: {len(no_changes)}"
    )

    return new_records, scd2_changes, soft_updates, no_changes


# ============================================
# STEP 5: APPLY SCD TYPE 2 LOGIC
# ============================================
def apply_scd2(conn, current_dim, staging,
               new_records, scd2_changes, soft_updates):

    cursor = conn.cursor()

    # BUG FIX 5: All dates must be strings for pyodbc ODBC driver
    today = date.today().strftime('%Y-%m-%d')
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    inserted = 0
    expired = 0
    soft_updated = 0

    # ------------------------------------------
    # 5A: INSERT brand new customers
    # ------------------------------------------
    for cust_id in new_records:
        row = staging.loc[cust_id]

        cursor.execute("""
            INSERT INTO dim_customer
            (customer_id, first_name, last_name, email,
             city, state, customer_segment,
             effective_date, expiry_date, is_current)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        int(cust_id),
        str(row['first_name']),
        str(row['last_name']),
        str(row['email']),
        str(row['city']),
        str(row['state']),
        str(row['customer_segment']),
        str(row['source_date']),   # BUG FIX 5: string date
        config.HIGH_DATE)                 # BUG FIX 5: string date

        # BUG FIX 6: row.to_dict() can have non-serializable types
        # Convert all values to str before json.dumps
        log_audit(cursor, cust_id, 'INSERT_NEW', [],
                  None,
                  {k: str(v) for k, v in row.to_dict().items()})
        inserted += 1

    # ------------------------------------------
    # 5B: SCD TYPE 2 UPDATE
    # Two operations per change:
    #   1. EXPIRE old row  (expiry_date = yesterday, is_current = 0)
    #   2. INSERT new row  (effective_date = today,  is_current = 1)
    # ------------------------------------------
    for cust_id, changed_cols in scd2_changes:
        dim_row = current_dim.loc[cust_id]
        stg_row = staging.loc[cust_id]

        old_values = {col: str(dim_row[col]) for col in changed_cols}
        new_values = {col: str(stg_row[col]) for col in changed_cols}

        # OPERATION 1: Expire old record
        cursor.execute("""
            UPDATE dim_customer
            SET expiry_date = ?,
                is_current = 0,
                updated_at = GETDATE()
            WHERE customer_id = ?
              AND is_current = 1
        """,
        yesterday,       # BUG FIX 5: string, not date object
        int(cust_id))
        expired += 1

        # OPERATION 2: Insert new active record
        cursor.execute("""
            INSERT INTO dim_customer
            (customer_id, first_name, last_name, email,
             city, state, customer_segment,
             effective_date, expiry_date, is_current)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        int(cust_id),
        str(stg_row['first_name']),
        str(stg_row['last_name']),
        str(stg_row['email']),
        str(stg_row['city']),
        str(stg_row['state']),
        str(stg_row['customer_segment']),
        str(stg_row['source_date']),   # BUG FIX 5: string date
        config.HIGH_DATE)                     # BUG FIX 5: string date
        inserted += 1

        log_audit(cursor, cust_id, 'SCD2_UPDATE',
                  changed_cols, old_values, new_values)

    # ------------------------------------------
    # 5C: SOFT UPDATE (non-tracked columns only)
    # In-place update — no new history row created
    # ------------------------------------------
    for cust_id in soft_updates:
        stg_row = staging.loc[cust_id]
        dim_row = current_dim.loc[cust_id]

        cursor.execute("""
            UPDATE dim_customer
            SET email = ?,
                updated_at = GETDATE()
            WHERE customer_id = ?
              AND is_current = 1
        """,
        str(stg_row['email']),
        int(cust_id))

        log_audit(cursor, cust_id, 'SOFT_UPDATE',
                  ['email'],
                  {'email': str(dim_row['email'])},
                  {'email': str(stg_row['email'])})
        soft_updated += 1

    conn.commit()
    logger.info(
        f"Applied → "
        f"Inserted: {inserted} | "
        f"Expired: {expired} | "
        f"Soft Updated: {soft_updated}"
    )

    return inserted, expired, soft_updated


# ============================================
# AUDIT LOGGING HELPER
# ============================================
def log_audit(cursor, cust_id, operation,
              changed_cols, old_val, new_val):
    cursor.execute("""
        INSERT INTO scd_audit_log 
        (customer_id, operation, changed_columns, 
         old_value, new_value)
        VALUES (?, ?, ?, ?, ?)
    """,
    int(cust_id),
    str(operation),
    ','.join(changed_cols) if changed_cols else None,
    json.dumps(old_val) if old_val else None,
    json.dumps(new_val) if new_val else None)


# ============================================
# MAIN ORCHESTRATOR
# ============================================
def run_scd2_pipeline(filepath):
    logger.info("=" * 50)
    logger.info(f"SCD2 Pipeline Started — {datetime.now()}")
    logger.info(f"Source file: {filepath}")

    conn = get_conn()

    try:
        load_to_staging(conn, filepath)

        current_dim = get_current_dimension(conn)
        staging = get_staging_data(conn)

        logger.info(f"Current dimension: {len(current_dim)} active records")
        logger.info(f"Staging records:   {len(staging)}")

        new_records, scd2_changes, soft_updates, no_changes = \
            detect_changes(current_dim, staging)

        inserted, expired, soft_updated = apply_scd2(
            conn, current_dim, staging,
            new_records, scd2_changes, soft_updates
        )

        logger.info("Pipeline completed successfully ✓")
        logger.info("=" * 50)

    except Exception as e:
        conn.rollback()
        logger.error(f"Pipeline FAILED: {str(e)}")
        raise
    finally:
        conn.close()


# ============================================
# ENTRY POINT
# ============================================
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        run_scd2_pipeline(sys.argv[1])
    else:
        files = [
            'data/initial_load.csv',
            'data/delta_load_1.csv',
            'data/delta_load_2.csv',
            'data/delta_load_3.csv'
        ]
        for f in files:
            if os.path.exists(f):
                run_scd2_pipeline(f)
                print()
            else:
                logger.warning(f"File not found, skipping: {f}")