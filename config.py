DB_CONFIG = {
    'DRIVER': 'SQL SERVER',
    'SERVER': 'localhost\\SQLExpress',
    'DATABASE': 'SCD_Project',
    'Trusted_Connection': 'yes'
}

# ============================================
# COLUMNS THAT TRIGGER AN SCD2 UPDATE
# ============================================
SCD2_COLUMNS = ['city', 'state', 'customer_segment']
NON_TRACKED_COLUMNS = ['email']
HIGH_DATE = '9999-12-31'