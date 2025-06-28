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

load_dotenv()

alphavantagekey = os.getenv('ALPHAVANTAGE_API_KEY')
serpapi_key = os.getenv('SERPAPI_KEY')

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]

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

tools = [get_stock_data, get_balance_sheet, get_income_statement, web_search]

model = ChatOpenAI(
    model='gpt-4o-mini',
    temperature=0,
    openai_api_key=os.getenv('OPENAI_KEY'),
).bind_tools(tools)

def model_call(state: AgentState) -> AgentState:
    system_prompt = SystemMessage(
        content="You are a financial analysis assistant. You can fetch stock data, balance sheets, and income statements using the tools provided."
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

inputs = {"messages": [("user", "What is the stock price of QQQ, the balance sheet of NVDIA, and the income statement of TSLA, and the latest stock news of American Airlines?")]}
print_stream(app.stream(inputs, stream_mode="values"))