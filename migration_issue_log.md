# Migration Issues Log
## Project: SCD Type 2 Customer Dimension Pipeline
**Tech Stack:** Python 3.11 | SQL Server 2019 (SQLExpress) | pyodbc | pandas  
**Date:** March 2026  
**Status:** All issues resolved 

---

## How to Read This Log

Each issue follows this structure:
- **What happened** — the exact error or symptom
- **Root cause** — why it actually happened
- **Fix applied** — exactly what was changed
- **Lesson learned** — what this teaches about production ETL

---

## Issue #1 — pyodbc SQLBindParameter Error on Date Objects

**Severity:** Critical (pipeline could not run)  
**Phase:** load_to_staging() — INSERT into stg_customer  
**Error message:**
```
pyodbc.Error: ('HYC00', '[HYC00] [Microsoft][ODBC SQL Server Driver]
Optional feature not implemented (0) (SQLBindParameter)')
```

**What happened:**  
The pipeline crashed immediately when trying to INSERT rows into stg_customer.
The error pointed to the cursor.execute() call inside the staging loop.

**Root cause:**  
Python's `datetime.date` objects cannot be passed directly as parameters
to the Microsoft ODBC SQL Server Driver via pyodbc. The driver's
SQLBindParameter function does not implement binding for native Python
date types — it expects either a full datetime object or a plain string
in 'YYYY-MM-DD' format.

The affected code:
```python
# BROKEN — passing Python date object directly
df['source_date'] = pd.to_datetime(df['source_date']).dt.date
cursor.execute("INSERT INTO stg_customer (..., source_date) VALUES (..., ?)", 
               row['source_date'])  # date object → crash
```

**Fix applied:**  
Converted all date values to strings using strftime before passing to pyodbc.
Applied this fix consistently to source_date, effective_date, expiry_date,
and yesterday/today variables throughout the pipeline.

```python
# FIXED — converting date to string first
df['source_date'] = pd.to_datetime(df['source_date']).dt.strftime('%Y-%m-%d')

HIGH_DATE = '9999-12-31'                              # string, not date()
today     = date.today().strftime('%Y-%m-%d')         # string
yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # string
```

**Lesson learned:**  
When using pyodbc with the Microsoft ODBC SQL Server Driver, always pass
dates as strings in 'YYYY-MM-DD' format. This is a known driver limitation
that does not affect all ODBC drivers — PostgreSQL's psycopg2, for example,
handles native date objects natively. Always verify driver compatibility
when switching databases in an ETL pipeline.

---

## Issue #2 — Column Name Mismatch: source_guide vs source_date

**Severity:** High (pipeline could not load staging data)  
**Phase:** load_to_staging() — pd.read_csv()  
**Error message:**
```
KeyError: 'source_date'
pandas.core.indexes.base.py — IndexEngine.get_loc
```

**What happened:**  
The pipeline crashed when trying to access df['source_date'] after reading
the CSV. The column simply did not exist in the DataFrame.

**Root cause:**  
The generate_data.py script used Faker with the Indian locale ('en_IN').
The Faker library generated an unexpected column named 'source_guide'
instead of the expected 'source_date' due to a locale-specific field
naming behavior. The CSV was generated correctly but the column name
did not match what the pipeline expected.

Confirmed by inspecting the CSV header:
```
customer_id, first_name, last_name, email, city, state, 
customer_segment, source_guide   ← should have been source_date
```

**Fix applied:**  
Added a runtime column rename at the start of load_to_staging() to handle
the mismatch without modifying the source CSV files:

```python
# Rename source_guide → source_date if present
if 'source_guide' in df.columns:
    df.rename(columns={'source_guide': 'source_date'}, inplace=True)
```

**Lesson learned:**  
Never assume source data column names are stable. In production ETL,
source systems frequently rename columns without warning. Adding a
column mapping/rename layer at the ingestion stage — before any
transformation logic — makes pipelines resilient to upstream schema changes.
This is a standard pattern in enterprise ETL design.

---

## Issue #3 — TRUNCATE TABLE Missing from Staging Load

**Severity:** Medium (data correctness issue, no crash)  
**Phase:** load_to_staging()  
**Symptom:**  
On the second pipeline run, staging contained duplicate records —
rows from both the first and second CSV loads were present simultaneously.
This caused the change detection logic to flag records as duplicates
and produced incorrect NEW/SCD2 counts.

**Root cause:**  
The initial load_to_staging() function was missing a TRUNCATE TABLE
statement before the INSERT loop. Without truncation, every pipeline
run appended new rows on top of existing staging data instead of
replacing it.

```python
# BROKEN — missing truncation, rows stack up
for _, row in df.iterrows():
    cursor.execute("INSERT INTO stg_customer ...")
```

**Fix applied:**  
Added TRUNCATE TABLE as the first operation inside load_to_staging(),
before any INSERT occurs:

```python
# FIXED — always start with a clean slate
cursor.execute("TRUNCATE TABLE stg_customer")

for _, row in df.iterrows():
    cursor.execute("INSERT INTO stg_customer ...")
```

**Lesson learned:**  
Staging tables are designed to be transient — they should always reflect
exactly the current batch, nothing more. The standard pattern is
TRUNCATE → LOAD → VALIDATE → PROCESS. Skipping the TRUNCATE step
is one of the most common ETL bugs in production because it does not
cause an immediate crash — it silently corrupts downstream logic.
Always treat TRUNCATE as mandatory, not optional, for staging tables.

---

## Issue #4 — pyodbc.connect() Passed a Dictionary Instead of a String

**Severity:** Critical (connection could not be established)  
**Phase:** get_conn() — query_dim.py  
**Error message:**
```
TypeError: argument 1 must be a string or unicode object
```

**What happened:**  
query_dim.py crashed immediately on startup before running any query.
The connection function failed to open a database connection.

**Root cause:**  
When refactoring the connection logic to use a config.py file, the
DB_CONFIG was defined as a Python dictionary:

```python
# config.py
DB_CONFIG = {
    'driver': 'SQL Server',
    'server': 'localhost\\SQLExpress',
    'database': 'SCD_Project',
    'trusted_connection': 'yes'
}
```

But pyodbc.connect() requires a single connection string, not a dict:
```python
# BROKEN — passing dict directly
return pyodbc.connect(config.DB_CONFIG)   # TypeError
```

**Fix applied:**  
Changed get_conn() to build a proper connection string:

```python
# FIXED — explicit connection string
def get_conn():
    return pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=localhost\\SQLExpress;'
        'DATABASE=SCD_Project;'
        'Trusted_Connection=yes;'
    )
```

**Lesson learned:**  
pyodbc.connect() only accepts a connection string or keyword arguments —
not a dictionary object. When using a config file for database settings,
either build the connection string dynamically from the dict values,
or use SQLAlchemy's create_engine() which accepts connection parameters
as a dictionary. This is a common mistake when migrating from other
ORMs like SQLAlchemy to raw pyodbc.

---

## Issue #5 — Typos in SQL Queries Causing Column Not Found Errors

**Severity:** Medium (query execution failure)  
**Phase:** query_dim.py — multiple functions  
**Errors:**
```
# In get_customer_snapshot():
column 'forst_name' does not exist   (should be first_name)

# In get_audit_summary():
column 'changed_coulmns' does not exist  (should be changed_columns)
```

**Root cause:**  
Manual typing errors in SQL query strings inside Python. SQL queries
embedded in Python strings have no IDE autocomplete or compile-time
checking — typos only surface at runtime when the query is executed.

**Fix applied:**  
Corrected both column name typos:
```python
# FIXED in get_customer_snapshot():
SELECT customer_id, first_name, last_name, ...  # was: forst_name

# FIXED in get_audit_summary():
SELECT operation, changed_columns, ...           # was: changed_coulmns
```

**Lesson learned:**  
SQL strings inside Python have no syntax highlighting or validation.
Best practices to avoid this:
1. Always run each SQL query in SSMS first to confirm it works,
   then copy it into Python
2. Use column aliases that match your DataFrame expectations
3. In production, store SQL queries in separate .sql files and
   load them at runtime — this allows proper SQL syntax checking
   in SSMS before deployment

---

## Summary Table

| # | Issue | Phase | Severity | Type |
|---|---|---|---|---|
| 1 | pyodbc date binding (SQLBindParameter) | load_to_staging | Critical | Driver compatibility |
| 2 | source_guide vs source_date column name | load_to_staging | High | Schema mismatch |
| 3 | Missing TRUNCATE before staging INSERT | load_to_staging | Medium | Logic error |
| 4 | Dict passed to pyodbc.connect() | get_conn | Critical | Type error |
| 5 | SQL column name typos | query_dim | Medium | Typo |

---

## What This Project Taught Me

1. **Driver-level compatibility matters** — switching ODBC drivers 
   can break date handling silently. Always test date inserts first.

2. **Source schema changes are inevitable** — building column rename 
   logic at ingestion makes pipelines resilient to upstream changes.

3. **Staging tables must always be truncated** — never assume they 
   are clean. Treat TRUNCATE as a mandatory first step, not optional.

4. **Test SQL in SSMS before embedding in Python** — SQL inside 
   Python strings gets no validation until runtime. Validate first.

5. **Document bugs as you hit them** — this log was written during 
   development, not after. Real ETL developers maintain issue logs 
   throughout the project, not as an afterthought.