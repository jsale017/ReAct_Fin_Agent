import duckdb
import os
from dotenv import load_dotenv
from typing import List, Optional
from datetime import datetime

load_dotenv()

class FinancialAgentDB:
    def __init__(self, motherducker_token: Optional[str] = None):
        """Initialize the database connection."""
        self.token = motherducker_token or os.getenv("MOTHERDUCKER_TOKEN")
        if not self.token:
            raise ValueError("MotherDuck token is required.")
        
        # First connect to MotherDuck without specifying a database
        self.conn = duckdb.connect(f"md:?motherduck_token={self.token}")
        
        # Create the database if it doesn't exist
        self.conn.execute("CREATE DATABASE IF NOT EXISTS finreact_db")
        
        # Now use the database
        self.conn.execute("USE finreact_db")

    def setup_db(self):
        """Set up the database schema."""
        # Create a counter table for generating IDs
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS id_counters (
                table_name VARCHAR PRIMARY KEY,
                last_id INTEGER DEFAULT 0
            )
        """)
        
        # Initialize counters
        tables = ['users', 'favorite_stocks', 'user_queries', 'agent_responses', 'query_stocks']
        for table in tables:
            self.conn.execute("""
                INSERT INTO id_counters (table_name, last_id) 
                VALUES (?, 0) 
                ON CONFLICT (table_name) DO NOTHING
            """, [table])
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                email VARCHAR UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS favorite_stocks (
                favorite_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                stock_symbol VARCHAR NOT NULL,
                price_threshold_low DECIMAL(10, 2),
                price_threshold_high DECIMAL(10, 2),
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                UNIQUE (user_id, stock_symbol)
            )
        """)

        self.conn.execute("""
                CREATE TABLE IF NOT EXISTS user_queries (
                    query_id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    query_text TEXT NOT NULL,
                    query_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    query_type VARCHAR,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_responses (
                response_id INTEGER PRIMARY KEY,
                query_id INTEGER NOT NULL,
                response_text TEXT NOT NULL,
                response_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tools_used TEXT,
                execution_time_ms INTEGER,
                FOREIGN KEY (query_id) REFERENCES user_queries(query_id)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS query_stocks (
                id INTEGER PRIMARY KEY,
                query_id INTEGER NOT NULL,
                stock_symbol VARCHAR NOT NULL,
                FOREIGN KEY (query_id) REFERENCES user_queries(query_id)
            )
        """)

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_emails ON users(email)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_favorite_stocks_user ON favorite_stocks(user_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_queries_user ON user_queries(user_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_queries_timestamp ON user_queries(query_timestamp)")
        print("Database schema set up successfully.")

    def _get_next_id(self, table_name: str) -> int:
        """Get the next ID for a table"""
        # Update and return the new ID atomically
        self.conn.execute("""
            UPDATE id_counters 
            SET last_id = last_id + 1 
            WHERE table_name = ?
        """, [table_name])
        
        result = self.conn.execute("""
            SELECT last_id FROM id_counters WHERE table_name = ?
        """, [table_name]).fetchone()
        
        return result[0]

    def create_user(self, email: str) -> int:
        """Create a new user and return their user_id"""
        # Check if user already exists
        existing = self.conn.execute("""
            SELECT user_id FROM users WHERE email = ?
        """, [email]).fetchone()
        
        if existing:
            return existing[0]
        
        # Get next ID and insert
        user_id = self._get_next_id('users')
        self.conn.execute("""
            INSERT INTO users (user_id, email) 
            VALUES (?, ?)
        """, [user_id, email])
        
        return user_id
    
    def add_favorite_stock(self, user_id: int, stock_symbol: str, 
                          price_threshold_low: Optional[float] = None,
                          price_threshold_high: Optional[float] = None) -> bool:
        """Add a favorite stock for a user (max 5)"""
        # Check if user already has 5 favorites
        count = self.conn.execute("""
            SELECT COUNT(*) FROM favorite_stocks WHERE user_id = ?
        """, [user_id]).fetchone()[0]
        
        if count >= 5:
            print(f"User {user_id} already has 5 favorite stocks. Remove one before adding more.")
            return False
        
        # Check if stock already exists for user
        existing = self.conn.execute("""
            SELECT 1 FROM favorite_stocks 
            WHERE user_id = ? AND stock_symbol = ?
        """, [user_id, stock_symbol.upper()]).fetchone()
        
        if existing:
            print(f"Stock {stock_symbol} is already in favorites for user {user_id}")
            return False
        
        # Get next ID and insert
        favorite_id = self._get_next_id('favorite_stocks')
        self.conn.execute("""
            INSERT INTO favorite_stocks 
            (favorite_id, user_id, stock_symbol, price_threshold_low, price_threshold_high)
            VALUES (?, ?, ?, ?, ?)
        """, [favorite_id, user_id, stock_symbol.upper(), price_threshold_low, price_threshold_high])
        
        return True
    
    def log_query(self, user_id: int, query_text: str, query_type: str = 'mixed') -> int:
        """Log a user query and return the query_id"""
        query_id = self._get_next_id('user_queries')
        self.conn.execute("""
            INSERT INTO user_queries (query_id, user_id, query_text, query_type)
            VALUES (?, ?, ?, ?)
        """, [query_id, user_id, query_text, query_type])
        
        return query_id
    
    def log_response(self, query_id: int, response_text: str, 
                    tools_used: List[str], execution_time_ms: int):
        """Log an agent response"""
        import json
        response_id = self._get_next_id('agent_responses')
        tools_json = json.dumps(tools_used)
        self.conn.execute("""
            INSERT INTO agent_responses 
            (response_id, query_id, response_text, tools_used, execution_time_ms)
            VALUES (?, ?, ?, ?, ?)
        """, [response_id, query_id, response_text, tools_json, execution_time_ms])
    
    def log_query_stocks(self, query_id: int, stock_symbols: List[str]):
        """Log which stocks were queried"""
        for symbol in stock_symbols:
            stock_id = self._get_next_id('query_stocks')
            self.conn.execute("""
                INSERT INTO query_stocks (id, query_id, stock_symbol)
                VALUES (?, ?, ?)
            """, [stock_id, query_id, symbol.upper()])

    def get_user_favorites(self, user_id: int):
        """Get a user's favorite stocks with thresholds"""
        return self.conn.execute("""
            SELECT stock_symbol, price_threshold_low, price_threshold_high, added_at
            FROM favorite_stocks
            WHERE user_id = ?
            ORDER BY added_at DESC
        """, [user_id]).fetchall()
    
    def get_user_query_history(self, user_id: int, limit: int = 10):
        """Get a user's recent queries with responses"""
        return self.conn.execute(
            """
            SELECT
                q.query_text,
                q.query_timestamp,
                r.response_text,
                r.tools_used,
                r.execution_time_ms
            FROM user_queries q
            JOIN agent_responses r ON q.query_id = r.query_id
            WHERE q.user_id = ?
            ORDER BY q.query_timestamp DESC
            LIMIT ?
        """, [user_id, limit]).fetchall() 
        
    def remove_favorite_stock(self, user_id: int, stock_symbol: str):
        """Remove a stock from user's favorites"""
        self.conn.execute("""
            DELETE FROM favorite_stocks 
            WHERE user_id = ? AND stock_symbol = ?
        """, [user_id, stock_symbol.upper()])
    
    def update_thresholds(self, user_id: int, stock_symbol: str,
                         price_threshold_low: Optional[float] = None,
                         price_threshold_high: Optional[float] = None):
        """Update price thresholds for a favorite stock"""
        self.conn.execute("""
            UPDATE favorite_stocks
            SET price_threshold_low = ?, 
                price_threshold_high = ?
            WHERE user_id = ? AND stock_symbol = ?
        """, [price_threshold_low, price_threshold_high, user_id, stock_symbol.upper()])
    
    def close(self):
        """Close the database connection"""
        self.conn.close()

if __name__ == "__main__":
    db = FinancialAgentDB()
    db.setup_db()
    print("Database setup complete.")
    db.close()
    print("Connection closed.")