import pandas as pd
import config
import pyodbc

# Database Connection
def get_conn():
    return pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=localhost\\SQLExpress;'
        'DATABASE=SCD_Project;'
        'Trusted_Connection=yes;'
    )

# Q1: All active customer
def get_active_customer():
    conn = get_conn()
    query = """
        select customer_id, first_name, last_name, city, state, customer_segment, effective_date
        from dim_customer where is_current = 1 order by customer_id;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Q2: full history for one customer
def  get_customer_history(customer_id):
    conn = get_conn()
    query = """
        select customer_sk, customer_id, first_name, last_name, city, state, customer_segment, 
        effective_date, is_current, expiry_date from  dim_customer where customer_id = ? order by effective_date;    
    """
    
    df = pd.read_sql(query, conn, params=[customer_id])
    conn.close()
    return df

# Q3: point-in-time snapshot
def get_customer_snapshot(snapshot_date):
    conn = get_conn()
    query = """
        select customer_id, first_name, last_name, city, state, customer_segment
        from dim_customer where ? between effective_date and expiry_date order by customer_id;
    """
    
    df = pd.read_sql(query, conn, params=[snapshot_date])
    conn.close()
    return df

# Q4: customer who changed segment
def get_segment_changer():
    conn = get_conn()
    query = """
        select customer_id, customer_segment as current_segment, LAG(customer_segment) over (partition by customer_id order by effective_date)
        as previous_segment, effective_date as changed_date from dim_customer where customer_id in (select customer_id from dim_customer
        group by customer_id having count(*)>1);
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df[df['previous_segment'].notna()]

# Q5. audit summary
def get_audit_summary():
    conn = get_conn()
    query = """
        select operation, changed_columns, count(*) as count, max(run_date) as last_run
        from scd_audit_log group by operation, changed_columns order by last_run desc;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Main 
if __name__ == "__main__":
    
    print("-- Active Customer (first 5) --")
    df = get_active_customer()
    print(df.head())
    print(f"Total active customers: {len(df)}\n")
    
    print("-- History for customer through ID --")
    his_inp = int(input("Enter Customer ID: "))
    print(get_customer_history(his_inp))
    print()
    
    print("-- Snapshot On Particular Date --")
    snap = input("Enter Date (format: YYYY-MM-DD): ")
    df = get_customer_snapshot(snap)
    print(df.head())
    print(f"Total customer on that date: {len(df)}\n")
    
    print("-- Segment Changers --")
    print(get_segment_changer().head())
    print()
    
    print("-- Audit Summary --")
    print(get_audit_summary())