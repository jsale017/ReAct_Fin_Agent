from typing import Annotated, Sequence, TypedDict
from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, ToolMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool, Tool
from langgraph.graph.message import add_messages
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
import os
import requests
from langchain_community.utilities import SerpAPIWrapper
from db import FinancialAgentDB
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

load_dotenv()

alphavantagekey = os.getenv('ALPHAVANTAGE_API_KEY')
serpapi_key = os.getenv('SERPAPI_KEY')
gmail_user = os.getenv("EMAIL")
gmail_password = os.getenv("PASSWORD")
class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    user_id: int
    user_email: str

@tool
def get_stock_data(symbol: str, alphavantagekey: str):
    """Fetches the stock data"""
    url = f'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': alphavantagekey
    }
    response = requests.get(url, params=params)
    return response.json()

@tool
def web_search(query: str, serpapi_api_key: str):
    """Performs a web search to extract the latest stock news."""
    search_wrapper = SerpAPIWrapper(serpapi_api_key=serpapi_key)
    search_results = search_wrapper.run(query)
    return search_results

@tool
def get_balance_sheet(symbol: str, alphavantagekey: str):
    """Fetches balance sheet data for a given stock symbol."""
    url = f'https://www.alphavantage.co/query'
    params = {
        'function': 'BALANCE_SHEET',
        'symbol': symbol,
        'apikey': alphavantagekey
    }
    response = requests.get(url, params=params)
    return response.json()

@tool
def get_income_statement(symbol: str, alphavantagekey: str):
    """Fetches income statement data for a given stock symbol."""
    url = f'https://www.alphavantage.co/query'
    params = {
        'function': 'INCOME_STATEMENT',
        'symbol': symbol,
        'apikey': alphavantagekey
    }
    response = requests.get(url, params=params)
    return response.json()

@tool
def get_user_favorites(user_id: int):
    """FEtch the user's favorite stocks from the database."""
    db = FinancialAgentDB()
    favorites = db.get_user_favorites(user_id)
    db.close()
    return favorites

@tool
def add_favorite_stock(user_id: int, stock_symbol: str,
                       price_threshold_low: float = None,
                       price_threshold_high: float = None):
    """Add a stock to the user's favorites in the database."""
    db = FinancialAgentDB()
    result = db.add_favorite_stock(user_id, stock_symbol, price_threshold_low, price_threshold_high)
    db.close()
    return {"success": result, "message": "Stock added to favorites." if result else "Failed to add stock to favorites."}

@tool
def remove_favorite_stock(user_id: int, stock_symbol: str):
    """Remove a stock from the user's favorites in the db"""
    db = FinancialAgentDB()
    db.remove_favorite_stock(user_id, stock_symbol)
    db.close()
    return {"success": True, "message": f"Removed {stock_symbol} from favorites."}

@tool
def update_stock_thresholds(user_id: int, stock_symbol: str,
                            price_threshold_low: float = None,
                            price_threshold_high: float = None):
    """Update price thresholds for a favorite stock."""
    db = FinancialAgentDB()
    db.update_thresholds(user_id, stock_symbol,
                         price_threshold_low, price_threshold_high)
    db.close()
    return {"success": True, "message": f"Updated thresholds for {stock_symbol}"}

@tool
def get_query_history(user_id: int, limit: int = 10):
    """Get the user's recent query history."""
    db = FinancialAgentDB()
    history = db.get_user_query_history(user_id, limit)
    db.close()
    return history

@tool
def send_email(recipient_email: str, subject: str, body: str):
    """Send a email with financial information to the user"""
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

tools = [get_stock_data, get_balance_sheet, get_income_statement, web_search, get_user_favorites, add_favorite_stock, remove_favorite_stock, update_stock_thresholds, get_query_history, send_email]

model = ChatOpenAI(
    model='gpt-4o-mini',
    temperature=0,
    openai_api_key=os.getenv('OPENAI_KEY'),
).bind_tools(tools)

def model_call(state: AgentState) -> AgentState:
    system_prompt = SystemMessage(
        content=f"""You are a financial analysis assistant. You can fetch stock data, balance sheets, and income statements using the tools provided.
        Current user email: {state.get('user_email')}
        User ID: {state.get('user_id')}
        
        You can manage the user's favorite stocks:
        - Add stocks to favorites (max 5) with optional price thresholds
        - Remove stocks from favorites
        - Update price thresholds for existing favorites
        - View all favorite stocks
        - Access query history
        
        You can also send financial information via email. When asked to send an email:
        - Use the user's email from the state
        - Create a clear subject and body
        - Format the body with the requested financial information

        Always use the user_id from the state when calling database tools."""
    )
    response = model.invoke([system_prompt] + state["messages"])
    return {"messages": [response]}

def should_continue(state: AgentState):
    messages = state["messages"]
    last_message = messages[-1]
    if not last_message.tool_calls:
        return "end"
    else:
        return "continue"

def authenticate_user():
    """Get user email and create/retrieve user"""
    email = input("Enter your email addess to continue: ").strip()
    if not email or '@' not in email:
        raise ValueError("Invalid email address.")
        return None, None
    
    db = FinancialAgentDB()
    db.setup_db()
    user_id = db.create_user(email)
    db.close()

    print(f"Welcome! Your user ID is: {user_id}")
    return user_id, email

graph = StateGraph(AgentState)
graph.add_node("Agent", model_call)

tool_node = ToolNode(tools=tools)
graph.add_node("Tools", tool_node)

graph.set_entry_point("Agent")

graph.add_conditional_edges(
    "Agent",
    should_continue,
    {
        'continue': "Tools",
        'end': END
    },
)
graph.add_edge("Tools", "Agent")
app = graph.compile()


def print_stream(stream):
    for s in stream:
        message = s['messages'][-1]
        if isinstance(message, tuple):
            print(message)
        else:
            message.pretty_print()

def run_financial_agent(query: str, user_id: int, user_email: str):
    """Run the agent with user context and database logging"""
    db = FinancialAgentDB()
    
    # Log query
    query_id = db.log_query(user_id, query)
    
    # Prepare inputs
    inputs = {
        "messages": [("user", query)],
        "user_id": user_id,
        "user_email": user_email
    }
    
    # Track execution time
    import time
    start_time = time.time()
    tools_used = []
    response_text = ""
    
    # Run agent
    for s in app.stream(inputs, stream_mode="values"):
        message = s['messages'][-1]
        if isinstance(message, tuple):
            print(message)
            response_text= str(message[1])
        else:
            message.pretty_print()
            if hasattr(message, 'tool_calls') and message.tool_calls:
                for tool_call in message.tool_calls:
                    tools_used.append(tool_call['name'])
            
            if hasattr(message, 'content') and message.content:
                response_text = message.content

    execution_time = int((time.time() - start_time) * 1000)
    db.log_response(query_id, response_text, tools_used, execution_time)

    import re
    stock_pattern = r'\b[A-Z]{1,5}\b'
    potential_symols = re.findall(stock_pattern, query)
    if potential_symols:
        db.log_query_stocks(query_id, potential_symols)

    db.close()

if __name__ == "__main__":
    user_id, user_email = authenticate_user()
    if not user_id:
        exit()
    
    query = input("\nWhat would you like to know about stocks? ")
    
    run_financial_agent(query, user_id, user_email)
    
    while True:
        another = input("\nWould you like to ask another question? (y/n): ").lower()
        if another != 'y':
            break
        query = input("\nWhat would you like to know? ")
        run_financial_agent(query, user_id, user_email)