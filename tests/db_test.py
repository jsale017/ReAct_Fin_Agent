import os
from dotenv import load_dotenv
from datetime import datetime
import json
from db import FinancialAgentDB

def test_db():
    print("Testing FinancialAgentDB...")
    
    try:
        db = FinancialAgentDB()
        print("Database connection established successfully.")
        
        db.setup_db()
        print("Database schema set up successfully.")
        
        # Test user creation
        email = "josesalerno17@gmail.com"
        user_id = db.create_user(email)
        print(f"User created with ID: {user_id}")
        
        duplicated_user_id = db.create_user(email)
        print(f"Duplicated user creation returned ID: {duplicated_user_id}")
        
        test_stocks = [
            ("AAPL", 150.0, 200.0),
            ("GOOGL", 2500.0, None),
            ("AMZN", None, 3500.0),
            ("MSFT", 300.0, 400.0),
            ("TSLA", None, None)
        ]
        
        for stock_symbol, low, high in test_stocks:
            success = db.add_favorite_stock(user_id, stock_symbol, low, high)
            if success:
                print(f"Added favorite stock: {stock_symbol} for user {user_id}")
            else:
                print(f"Failed to add favorite stock: {stock_symbol} for user {user_id}")
        
        # Test retrieval of favorite stocks
        favorites = db.get_user_favorites(user_id)
        print(f"\nFavorite stocks for user {user_id}:")
        for stock in favorites:
            # Access tuple elements by index, not dictionary keys
            symbol = stock[0]
            low = stock[1] if stock[1] is not None else "None"
            high = stock[2] if stock[2] is not None else "None"
            added_at = stock[3]
            print(f"  - {symbol}: Low=${low}, High=${high}, Added: {added_at}")
        
        # Test logging a query
        test_query = "What is the current price of AAPL?"
        query_id = db.log_query(user_id, test_query, "stocks")
        print(f"\nLogged query with ID: {query_id}")
        
        db.log_query_stocks(query_id, ["AAPL"])
        print("Logged query stocks successfully.")
        
        # Test logging a response
        response_text = "The current price of AAPL is $150."
        tools_used = ["get_stock_data", "web_search"]
        execution_time_ms = 1234
        db.log_response(query_id, response_text, tools_used, execution_time_ms)
        print("Logged response successfully.")
        
        # Test updates to thresholds
        db.update_thresholds(user_id, "AAPL", 140.0, 210.0)
        print("\nUpdated thresholds for AAPL successfully.")
        
        # Test removal of a favorite stock
        db.remove_favorite_stock(user_id, "GOOGL")
        print("Removed favorite stock GOOGL successfully.")
        
        # Verify removal
        favorites_after = db.get_user_favorites(user_id)
        print(f"Remaining favorite stocks for user {user_id}: {len(favorites_after)}")
        
        # Get query history
        print("\nTesting query history retrieval...")
        history = db.get_user_query_history(user_id, limit=5)
        print(f"Retrieved {len(history)} queries from history")
        
        # Final statistics
        result = db.conn.execute("""
            SELECT 
                (SELECT COUNT(*) FROM users) as user_count,
                (SELECT COUNT(*) FROM favorite_stocks) as favorite_count,
                (SELECT COUNT(*) FROM user_queries) as query_count,
                (SELECT COUNT(*) FROM agent_responses) as response_count
        """).fetchone()
        
        print("\nDatabase Stats:")
        print(f"  - Users: {result[0]}")
        print(f"  - Favorite stocks: {result[1]}")
        print(f"  - Queries: {result[2]}")
        print(f"  - Responses: {result[3]}")
        
        print("\nWORKS: All tests completed successfully!")
        
    except Exception as e:
        print(f"\nError occurred during testing: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if 'db' in locals():
            db.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    load_dotenv()
    
    token = os.getenv("MOTHERDUCKER_TOKEN")
    if not token:
        raise ValueError("MOTHERDUCKER_TOKEN not set in .env file")
    else:
        print("MOTHERDUCKER_TOKEN loaded successfully.")
        print("-" * 50)
        
    test_db()