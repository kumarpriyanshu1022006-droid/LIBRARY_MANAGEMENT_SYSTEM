import pandas as pd
import sqlite3
import logging
from datetime import datetime
import os


# 1. Configuration & Setup

# Configure Logging
logging.basicConfig(
    filename='library_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_NAME = 'library.db'
CSV_FILE = 'Books_Issued.csv'

# Reference Data: Member Categories & Fine Rules
# Format: { 'Category': {'grace_days': int, 'rate_per_day': float, 'max_cap': float} }
FINE_RULES = {
    'student': {'grace_days': 2, 'rate_per_day': 1.5, 'max_cap': 50.0},
    'faculty': {'grace_days': 5, 'rate_per_day': 0.5, 'max_cap': 20.0}
}

# Mock User Reference Data (Maps UserID to Category)
# USER_CATEGORIES = {
#     'U001': 'student',
#     'U002': 'student',
#     'U003': 'faculty',
#     'U004': 'student'
# }
# 2. Database Initialization


def setup_database():
    """Initializes SQLite DB and creates necessary tables."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Transaction History Table (Primary Key prevents duplicates)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Transaction_History (
            TransactionID TEXT PRIMARY KEY,
            UserID TEXT,
            BookID TEXT,
            IssueDate TEXT,
            DueDate TEXT,
            ReturnDate TEXT,
            OverdueDays INTEGER,
            FineAmount REAL
        )
    ''')
    
    # User Account Table (Maintains current fine balances)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS User_Account (
            UserID TEXT PRIMARY KEY,
            Member_Category TEXT,
            Total_Fine REAL
        )
    ''')
    
    conn.commit()
    return conn



#  Extraction & Validation
def extract_and_validate_data(file_path):
    """Reads CSV, drops duplicates, and validates dates."""
    logging.info("Starting data extraction...")
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        logging.error(f"Input file {file_path} not found.")
        return None

    initial_count = len(df)
    
    # Validation 1: Remove Duplicate Transaction IDs
    df = df.drop_duplicates(subset=['TransactionID'], keep='first')
    logging.info(f"Removed {initial_count - len(df)} duplicate transactions.")
    
    # Convert dates to datetime objects
    date_cols = ['IssueDate', 'DueDate', 'ReturnDate']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Validation 2: Inconsistent dates (Issue date > Due date)
    invalid_dates = df[df['IssueDate'] > df['DueDate']]
    if not invalid_dates.empty:
        logging.warning(f"Found {len(invalid_dates)} records with IssueDate > DueDate. Dropping them.")
        df = df.drop(invalid_dates.index)
        
    logging.info(f"Data extraction and validation complete. {len(df)} records ready for processing.")
    return df

# 4. Data Processing (Transformation)

def calculate_fine(row):
    """Calculates overdue days and applies category-specific fine rules."""
    # Determine the end date for calculation (Return date or Current date if not returned)
    end_date = row['ReturnDate'] if pd.notnull(row['ReturnDate']) else pd.Timestamp(datetime.now().date())
    
    # Compute overdue days
    overdue_delta = end_date - row['DueDate']
    overdue_days = max(0, overdue_delta.days)
    
    # Fetch member rules
    category = USER_CATEGORIES.get(row['UserID'], 'student') # Default to student if unknown
    rules = FINE_RULES[category]
    
    fine_amount = 0.0
    if overdue_days > rules['grace_days']:
        # Apply fine only for days beyond the grace period
        chargeable_days = overdue_days - rules['grace_days']
        fine_amount = chargeable_days * rules['rate_per_day']
        # Apply Maximum Cap
        fine_amount = min(fine_amount, rules['max_cap'])
        
    return pd.Series([overdue_days, fine_amount, category])

def transform_data(df):
    """Applies fine calculations to the DataFrame."""
    logging.info("Starting data transformation and fine calculation...")
    
    # Apply calculation function to create new columns
    df[['OverdueDays', 'FineAmount', 'Member_Category']] = df.apply(calculate_fine, axis=1)
    
    # Convert dates back to strings for SQLite storage
    date_cols = ['IssueDate', 'DueDate', 'ReturnDate']
    for col in date_cols:
        df[col] = df[col].dt.strftime('%Y-%m-%d').replace('NaT', None)
        
    logging.info("Data transformation complete.")
    return df

# ==========================================
# 5. Data Loading
# ==========================================

def load_data(conn, df):
    """Loads transactions into DB and updates user balances."""
    logging.info("Loading data into SQLite database...")
    cursor = conn.cursor()
    
    new_records = 0
    updated_users = set()

    for _, row in df.iterrows():
        # Insert into Transaction_History (IGNORE skips if TransactionID already exists)
        try:
            cursor.execute('''
                INSERT INTO Transaction_History 
                (TransactionID, UserID, BookID, IssueDate, DueDate, ReturnDate, OverdueDays, FineAmount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (row['TransactionID'], row['UserID'], row['BookID'], row['IssueDate'], 
                  row['DueDate'], row['ReturnDate'], row['OverdueDays'], row['FineAmount']))
            new_records += 1
            updated_users.add((row['UserID'], row['Member_Category']))
        except sqlite3.IntegrityError:
            # Record already exists, do not duplicate the fine posting
            pass

    # Update User_Account balances using aggregate data from Transaction_History

    for user_id, category in updated_users:
        cursor.execute('''
            SELECT SUM(FineAmount) FROM Transaction_History WHERE UserID = ?
        ''', (user_id,))
        total_fine = cursor.fetchone()[0] or 0.0
        
        cursor.execute('''
            INSERT INTO User_Account (UserID, Member_Category, Total_Fine)
            VALUES (?, ?, ?)
            ON CONFLICT(UserID) DO UPDATE SET Total_Fine = excluded.Total_Fine
        ''', (user_id, category, total_fine))
        
    conn.commit()
    logging.info(f"Loaded {new_records} new transactions. Updated {len(updated_users)} user accounts.")


# 6. Reporting


def generate_reports(conn):
    """Generates the required summary reports from the database."""
    print("\n" + "="*40)
    print("LIBRARY MANAGEMENT ETL REPORT")
    print("="*40)
    
    # 1. Most Overdue Books
    print("\n--- Top 3 Most Overdue Books ---")
    query_overdue = '''
        SELECT BookID, UserID, OverdueDays 
        FROM Transaction_History 
        WHERE OverdueDays > 0 
        ORDER BY OverdueDays DESC LIMIT 3
    '''
    df_overdue = pd.read_sql(query_overdue, conn)
    print(df_overdue.to_string(index=False) if not df_overdue.empty else "No overdue books found.")
    
    # 2. Users with Highest Fines
    print("\n--- Users with Highest Fines ---")
    query_fines = '''
        SELECT UserID, Member_Category, Total_Fine 
        FROM User_Account 
        WHERE Total_Fine > 0 
        ORDER BY Total_Fine DESC LIMIT 3
    '''
    df_fines = pd.read_sql(query_fines, conn)
    print(df_fines.to_string(index=False) if not df_fines.empty else "No outstanding fines.")

    # 3. Summary Statistics of Overdue Patterns
    print("\n--- Summary Statistics of Overdue Patterns ---")
    query_stats = '''
        SELECT 
            COUNT(*) as Total_Overdue_Transactions,
            AVG(OverdueDays) as Average_Overdue_Days,
            MAX(OverdueDays) as Max_Overdue_Days,
            SUM(FineAmount) as Total_Fines_Generated
        FROM Transaction_History 
        WHERE OverdueDays > 0
    '''
    df_stats = pd.read_sql(query_stats, conn)
    # Rounding for cleaner output
    df_stats['Average_Overdue_Days'] = df_stats['Average_Overdue_Days'].round(2)
    print(df_stats.to_string(index=False) if not df_stats.empty else "No overdue statistics to report.")
    print("="*40 + "\n")


# Main Pipeline Execution


def run_pipeline():
    logging.info("--- Pipeline Execution Started ---")
    
    # 1. Setup DB
    conn = setup_database()
    
    # 2. Extract & Validate
    raw_df = extract_and_validate_data(CSV_FILE)
    if raw_df is not None and not raw_df.empty:
        # 3. Transform (Compute Overdue & Fines)
        processed_df = transform_data(raw_df)
        
        # 4. Load & Update Balances
        load_data(conn, processed_df)
        
        # 5. Reporting
        generate_reports(conn)
    else:
        logging.warning("Pipeline halted: No valid data extracted.")
        print("Pipeline failed. Check library_pipeline.log for details.")

    conn.close()
    logging.info("--- Pipeline Execution Finished ---\n")

if __name__ == "__main__":
    run_pipeline()