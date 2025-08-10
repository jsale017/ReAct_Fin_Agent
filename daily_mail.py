import os
import schedule
import time
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from langchain_community.utilities import SerpAPIWrapper
from db import FinancialAgentDB

load_dotenv()

alphavantagekey = os.getenv('ALPHAVANTAGE_API_KEY')
serapi_key = os.getenv('SERPAPI_KEY')
gmail_user = os.getenv("EMAIL")
gmail_password = os.getenv("PASSWORD")

class StockEmailer:
    def __init__(self):
        self.db = FinancialAgentDB()
        self.alphavantagekey = alphavantagekey
        self.serpapi_key = serapi_key

    def get_stock_data(self, symbol: str) -> Dict[str, Any]:
        """Fetches the stock data"""
        url = f"https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.alphavantagekey
        }

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if "Time Series (Daily)" in data:
                time_series = data["Time Series (Daily)"]
                latest_date = sorted(time_series.keys(), reverse=True)[0]
                latest_data = time_series[latest_date]

                return {
                    'symbol': symbol,
                    'date': latest_date,
                    'close': float(latest_data['4. close']),
                    'open': float(latest_data['1. open']),
                    'high': float(latest_data['2. high']),
                    'low': float(latest_data['3. low']),
                    'volume': int(latest_data['5. volume']),
                    'change': float(latest_data['4. close']) - float(latest_data['1. open']),
                    'change_percent': ((float(latest_data['4. close']) - float(latest_data['1. open'])) / float(latest_data['1. open'])) * 100
                }
            else:
                return {'symbol': symbol, 'error': 'No data found for this symbol.'}
        except Exception as e:
            return {'symbol': symbol, 'error': str(e)}
        
    def get_stock_news(self, symbol: str) -> List[Dict[str, str]]:
        """Fetch latest news for a stock symbol."""
        try:
            search_wrapper = SerpAPIWrapper(serpapi_api_key=self.serpapi_key)
            query = f"{symbol} stock news"
            search_results = search_wrapper.run(query)
            
            news_items = []
            if search_results:
                lines = search_results.split('\n')
                for i, line in enumerate(lines[:5]):
                    if line.strip():
                        news_items.append({
                            'title': line.strip(),
                            'index': i + 1
                        })

            return news_items
        except Exception as e:
            return [{'error': str(e)}]
        
    def check_price_alerts(self, stock_data: Dict, low_threshold: float, high_threshold: float) -> str:
        """Check if stock price crossed any thresholds."""
        alerts = []
        if low_threshold and stock_data.get('close', 0) <= low_threshold:
            alerts.append(f"PRICE DROPPED: {stock_data['symbol']} closed at {stock_data['close']}, below your low threshold of {low_threshold}.")
        if high_threshold and stock_data.get('close', 0) >= high_threshold:
            alerts.append(f"PRICE ROSE: {stock_data['symbol']} closed at {stock_data['close']}, above your high threshold of {high_threshold}.")
        return "\n".join(alerts) if alerts else None
    
    def format_stock_email(self, user_email: str, favorites_data: List[Dict]) -> str:
        """Format the email body with stock data"""
        today = datetime.now().strftime("%Y-%m-%d")
        body = f"""Good Evening!
        
        Heres is your daily stock update for {today}:\n\n
        """

        for favorite in favorites_data:
            stock_data = favorite['stock_data']
            news = favorite['news']
            alerts = favorite['alerts']

            if "error" in stock_data:
                body += f"\n{'='*50}\n"
                body += f"{favorite['symbol']}"
                body += f"\nError: {stock_data['error']}\n"
                continue

            body += f"\n{'='*50}\n"
            body += f"{stock_data['symbol']} - {stock_data['date']}\n"
            body += f"\n{'='*50}\n"

            change_emoji = "📈" if stock_data['change'] > 0 else "📉"
            body += f"Close: {stock_data['close']} {change_emoji}\n"
            body += f"Open: {stock_data['open']}\n"
            body += f"Change: {stock_data['change']} ({stock_data['change_percent']:.2f}%)\n"
            body += f"Volume: {stock_data['volume']}\n"

            if alerts:
                body += f"\nAlerts:\n{alerts}\n"
            
            body += "\nLatest News:\n"
            if news:
                for item in news:
                    body += f"{item['index']}. {item['title']}\n"
            else:
                body += "No news found.\n"

        body += "\n{'='*50}\n"
        body += "Have a great evening & keep on crushing it!\n"
        body += "Your Financial Agent"

        return body
    
    def send_email(self, recipient_email: str, subject: str, body: str) -> bool:
        """Send an email with financial information to the user"""
        try:
            msg = MIMEMultipart()
            msg['From'] = gmail_user
            msg['To'] = recipient_email
            msg['Subject'] = subject

            msg.attach(MIMEText(body, 'plain'))

            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(gmail_user, gmail_password)
            
            # send the email
            text = msg.as_string()
            server.sendmail(gmail_user, recipient_email, text)
            server.quit()

            return {"success": True, "message": f"Email sent successfully to {recipient_email}."}
        except Exception as e:
            return {"success": False, "message": f"Failed to send email: {str(e)}"}
        
    def daily_email_job(self):
        """Main function to send daily stock emails."""
        if datetime.now().weekday() > 4:
            print("Weekend detected, skipping daily email job.")
            return
        print("Starting daily email job...")
        users = self.db.get_all_users_with_favorites()
        for user in users:
            user_id = user['user_id']
            user_email = user['email']
            favorites = self.db.get_user_favorite_stocks(user_id)
            if not favorites:
                continue

            favorites_data = []
            for favorite in favorites:
                symbol = favorite['stock_symbol']
                low_threshold = favorite.get('price_threshold_low')
                high_threshold = favorite.get('price_threshold_high')
                stock_data = self.get_stock_data(symbol)
                news = self.get_stock_news(symbol)

                alerts = ""
                if 'error' not in stock_data:
                    alerts = self.check_price_alerts(stock_data, low_threshold, high_threshold)

                favorites_data.append({
                    'symbol': symbol,
                    'stock_data': stock_data,
                    'news': news,
                    'alerts': alerts
                })

            subject = f"Daily Stock Update for {datetime.now().strftime('%Y-%m-%d')}"
            body = self.format_stock_email(user_email, favorites_data)
            
            if self.send_email(user_email, subject, body):
                print(f"Email sent successsfully to {user_email}.")
                query_id = self.db.log_query(user_id, "Automated daily email sent", datetime.now())
                self.db.log_response(query_id, "Daily email sent", ['email', 'stock_data', 'news'], 0)

            else:
                print(f"Failed to send emaul to {user_email}.")
        print(f"Daily email job completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")
    
    def close(self):
        """Close the database connection."""
        self.db.close()

# Schedule the daily email job
def schedule_daily_email():
    emailer = StockEmailer()
    schedule.every().day.at("17:00").do(emailer.daily_email_job)

    print(f"Stock emailer scheduled to run daily at 17:00.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("Stopping the daily email scheduler.")
        emailer.close()

if __name__ == "__main__":
    StockEmailer().daily_email_job()
    