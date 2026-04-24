import sqlite3
import pandas as pd

def find_user_details(user_id):
    """Fetches and prints the account and transaction history for a specific user."""
    
    with sqlite3.connect('library.db') as conn:
        print(f"\n" + "="*45)
        print(f"   LIBRARY RECORD FOR USER: {user_id}")
        print("="*45)
        
        # 1. Fetch Account Information
        account_query = "SELECT * FROM User_Account WHERE UserID = ?"
        account_df = pd.read_sql(account_query, conn, params=(user_id,))
        
        if account_df.empty:
            print(f"No record found for user ID: {user_id}")
            print("="*45)
            return
            
        print("\n[USER ACCOUNT SUMMARY]")
        print(account_df.to_string(index=False))
        
        # 2. Fetch all their book transactions
        history_query = '''
            SELECT BookID, IssueDate, DueDate, ReturnDate, OverdueDays, FineAmount 
            FROM Transaction_History 
            WHERE UserID = ?
        '''
        history_df = pd.read_sql(history_query, conn, params=(user_id,))
        
        print("\n[TRANSACTION HISTORY]")
        if history_df.empty:
            print("No book transactions found for this user.")
        else:
            print(history_df.to_string(index=False))
        print("="*45)

if __name__ == "__main__":
    print("\n Welcome to the Library User Search!")
    

    while True:
      
        target_user = input("\nEnter a User ID to search (or type 'exit' to quit): ").strip().upper()
        
      
        if target_user == 'EXIT':
            print("Closing search. See ya!")
            break
            
      
        if target_user:
            find_user_details(target_user)