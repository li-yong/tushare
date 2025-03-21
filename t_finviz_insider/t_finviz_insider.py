#!/usr/bin/env python3
import time
import sys
import os
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def connect_to_chrome_and_list_tabs():
    """
    Connect to an already running Chrome instance via debug port 9222
    and list all open tab titles.
    """
    # Configure Chrome options to connect to the debugging port
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    
    try:
        # Connect to the existing Chrome instance
        driver = webdriver.Chrome(options=chrome_options)
        
        # Get all window handles (tabs)
        window_handles = driver.window_handles
        
        print(f"Found {len(window_handles)} tabs:")
        
        # Go through each tab and print the title
        for i, handle in enumerate(window_handles):
            driver.switch_to.window(handle)
            title = driver.title
            url = driver.current_url
            print(f"{i+1}. {title} - {url}")
        
        return driver, window_handles
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure Chrome is running with debug port enabled.")
        print("You can start Chrome with debug port using:")
        print("chrome.exe --remote-debugging-port=9222")
        return None, None

def switch_to_tab_by_url(target_url):
    """
    Connect to Chrome and switch to the tab with the specified URL.
    
    Args:
        target_url (str): The URL to switch to
        
    Returns:
        WebDriver or None: The driver if successful, None otherwise
    """
    # Configure Chrome options to connect to the debugging port
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    
    try:
        # Connect to the existing Chrome instance
        driver = webdriver.Chrome(options=chrome_options)
        
        # Get all window handles (tabs)
        window_handles = driver.window_handles
        
        # Go through each tab and look for the target URL
        for handle in window_handles:
            driver.switch_to.window(handle)
            current_url = driver.current_url
            
            if target_url in current_url:
                print(f"Successfully switched to: {driver.title} - {current_url}")
                return driver
                
        print(f"No tab with URL '{target_url}' found.")
        return None
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure Chrome is running with debug port enabled.")
        print("You can start Chrome with debug port using:")
        print("chrome.exe --remote-debugging-port=9222")
        return None

def extract_insider_table(driver):
    """
    Extract the insider-table data from the FinViz page and convert to DataFrame.
    
    Args:
        driver (WebDriver): The WebDriver instance
        
    Returns:
        DataFrame: Pandas DataFrame containing the table data
    """
    try:
        # Wait for table to be present
        wait = WebDriverWait(driver, 10)
        table = wait.until(EC.presence_of_element_located((By.ID, "insider-table")))
        
        # Get the HTML of the table
        html = table.get_attribute('outerHTML')
        
        # Use pandas read_html to parse the table HTML directly
        dfs = pd.read_html(html)
        
        # read_html returns a list of DataFrames, we want the first one
        df = dfs[0]
        
        # Merge transactions for the same ticker, owner, and transaction type
        # First, clean the Value ($) column
        df['Value ($)'] = df['Value ($)'].replace('', '0')
        df['Value ($)'] = df['Value ($)'].astype(str).str.replace(',', '')
        df['Value ($)'] = df['Value ($)'].str.replace('$', '')
        df['Value ($)'] = pd.to_numeric(df['Value ($)'], errors='coerce').fillna(0)
        
        # Clean the #Shares column similarly
        if '#Shares' in df.columns:
            df['#Shares'] = df['#Shares'].astype(str).str.replace(',', '')
            df['#Shares'] = pd.to_numeric(df['#Shares'], errors='coerce').fillna(0)
        
   
        
        print(f"Successfully extracted {len(df)} rows of insider trading data")
        return df
        
    except Exception as e:
        print(f"Error extracting table: {e}")
        return None

def get_market_caps(tickers):
    """
    Fetch market cap data for a list of ticker symbols using yfinance.
    First checks a local cache file before fetching from the API.
    
    Args:
        tickers (list): List of ticker symbols
        
    Returns:
        dict: Dictionary with ticker as key and market cap as value
    """
    market_caps = {}
    local_cache_file = "market_cap_cache.csv"
    cache_exists = False
    local_cache = {}
    
    # Check if we have a local cache file and load it
    try:
        if os.path.exists(local_cache_file):
            cache_exists = True
            cache_df = pd.read_csv(local_cache_file)
            # Convert DataFrame to dictionary
            local_cache = dict(zip(cache_df['Ticker'], cache_df['Market_Cap']))
            print(f"Loaded market cap data for {len(local_cache)} tickers from local cache")
    except Exception as e:
        print(f"Error loading local cache: {e}")
        local_cache = {}
    
    # Identify which tickers need to be fetched
    tickers_to_fetch = []
    for ticker in tickers:
        if ticker in local_cache:
            market_caps[ticker] = local_cache[ticker]
        else:
            tickers_to_fetch.append(ticker)
    
    if tickers_to_fetch:
        print(f"Fetching market cap data for {len(tickers_to_fetch)} new tickers...")
        
        try:
            # Use yfinance to get market cap data for all tickers at once
            tickers_str = ' '.join(tickers_to_fetch)
            data = yf.download(tickers_str, period="1d", group_by='ticker', progress=False)
            
            for ticker in tickers_to_fetch:
                try:
                    # Get the ticker info
                    ticker_info = yf.Ticker(ticker).info
                    
                    # Get market cap
                    if 'marketCap' in ticker_info:
                        market_cap = ticker_info['marketCap']
                        market_caps[ticker] = market_cap
                        # Add to local cache
                        local_cache[ticker] = market_cap
                    else:
                        print(f"Market cap not available for {ticker}")
                        market_caps[ticker] = None
                        local_cache[ticker] = None
                except Exception as e:
                    print(f"Error fetching data for {ticker}: {e}")
                    market_caps[ticker] = None
                
                # Add a small delay to avoid rate limiting
                time.sleep(0.1)
            
            # Save the updated cache to file
            try:
                cache_df = pd.DataFrame({
                    'Ticker': list(local_cache.keys()),
                    'Market_Cap': list(local_cache.values()),
                    'Last_Updated': [datetime.now().strftime("%Y-%m-%d")] * len(local_cache)
                })
                cache_df.to_csv(local_cache_file, index=False)
                print(f"Updated market cap cache saved to {local_cache_file}")
            except Exception as e:
                print(f"Error saving market cap cache: {e}")
        
        except Exception as e:
            print(f"Error fetching market cap data: {e}")
    else:
        print("All market cap data loaded from cache, no need to fetch from API")
    
    print(f"Successfully retrieved market cap data for {len(market_caps)} tickers")
    return market_caps

def add_market_cap_data(df):
    """
    Add market cap data and calculate Value/Market Cap ratio.
    
    Args:
        df (DataFrame): DataFrame containing insider trading data
        
    Returns:
        DataFrame: DataFrame with market cap data and Value/Market Cap ratio
    """
    try:
        # Make a copy of the dataframe
        df_with_mcap = df.copy()
        
        # Get unique tickers
        tickers = df_with_mcap['Ticker'].unique().tolist()
        
        # Get market cap data
        market_caps = get_market_caps(tickers)
        
        # Add market cap column
        df_with_mcap['Market Cap'] = df_with_mcap['Ticker'].map(market_caps)
        
        # Clean the Value ($) column
        value_col = 'Value ($)'
        df_with_mcap[value_col] = df_with_mcap[value_col].replace('', '0')
        df_with_mcap[value_col] = df_with_mcap[value_col].astype(str).str.replace(',', '')
        df_with_mcap[value_col] = df_with_mcap[value_col].str.replace('$', '')
        df_with_mcap[value_col] = pd.to_numeric(df_with_mcap[value_col], errors='coerce').fillna(0)
        
        # Calculate Value/Market Cap ratio (as percentage)
        df_with_mcap['Value/Market Cap (%)'] = None
        mask = df_with_mcap['Market Cap'] > 0  # Avoid division by zero
        df_with_mcap.loc[mask, 'Value/Market Cap (%)'] = (df_with_mcap.loc[mask, value_col] / df_with_mcap.loc[mask, 'Market Cap']) * 100
        
        # Format the Market Cap column (convert to billions)
        df_with_mcap['Market Cap (B)'] = df_with_mcap['Market Cap'].apply(lambda x: f"${x/1e9:.2f}B" if pd.notnull(x) and x > 0 else "N/A")
        
        return df_with_mcap
        
    except Exception as e:
        print(f"Error adding market cap data: {e}")
        import traceback
        traceback.print_exc()
        return df


def save_dataframe_to_csv(df, filename="insider_data.csv"):
    """
    Save the DataFrame to a CSV file.
    
    Args:
        df (DataFrame): The DataFrame to save
        filename (str): The filename to save to
    """
    try:
        df.to_csv(filename, index=False)
        print(f"Data successfully saved to {filename}")
    except Exception as e: 
        print(f"Error saving data: {e}")

def open_url_in_new_tab(url):
    """
    Open a URL in a new tab using the connected Chrome instance.
    
    Args:
        url (str): The URL to open in a new tab
        
    Returns:
        WebDriver or None: The driver with the new tab if successful, None otherwise
    """
    # Configure Chrome options to connect to the debugging port
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    
    try:
        # Connect to the existing Chrome instance
        driver = webdriver.Chrome(options=chrome_options)
        
        # Store the current window handle
        current_handle = driver.current_window_handle
        
        # Execute JavaScript to open a new tab
        driver.execute_script("window.open('about:blank', '_blank');")
        
        # Switch to the new tab (it will be the last one in the list)
        new_tab = driver.window_handles[-1]
        driver.switch_to.window(new_tab)
        
        # Navigate to the URL
        driver.get(url)
        print(f"Successfully opened {url} in a new tab")
        
        return driver, new_tab, current_handle
        
    except Exception as e:
        print(f"Error opening URL in new tab: {e}")
        return None, None, None

def close_tab_and_return(driver, original_handle):
    """
    Close the current tab and return to the original tab.
    
    Args:
        driver (WebDriver): The WebDriver instance
        original_handle (str): The handle of the tab to return to
        
    Returns:
        WebDriver: The driver switched back to the original tab
    """
    try:
        # Close the current tab
        driver.close()
        
        # Switch back to the original tab
        driver.switch_to.window(original_handle)
        print("Tab closed, returned to original tab")
        
        return driver
        
    except Exception as e:
        print(f"Error closing tab: {e}")
        return None

if __name__ == "__main__":
    # Check if URL is provided as command line argument
    if len(sys.argv) > 1 and sys.argv[1].startswith('http'):
        url_to_open = sys.argv[1]
        print(f"Opening URL: {url_to_open}")
        
        # Open the URL in a new tab
        driver, new_tab, original_handle = open_url_in_new_tab(url_to_open)
        
        if driver:
            # Wait for a moment to let the page load
            print("Page opened. Press Enter to close the tab...")
            input()
            
            # Close the tab and return to the original tab
            close_tab_and_return(driver, original_handle)
        
    else:
        # Define the FinViz insider trading URL
        finviz_url = "https://elite.finviz.com/insidertrading.ashx"
        
        # Connect to Chrome and switch to the FinViz tab
        driver = switch_to_tab_by_url(finviz_url)
        
        if driver:
            # Extract the insider table data
            df = extract_insider_table(driver)
            
            if df is not None:
                # Print the DataFrame head
                print("\nInsider Trading Data Preview:")
                print(df.head())
                
                # Add market cap data and calculate Value/Market Cap ratio
                df_with_mcap = add_market_cap_data(df)
                
                # Separate buy and sell transactions
                buy_df = df_with_mcap[df_with_mcap['Transaction'].str.lower() == 'buy']
                sell_df = df_with_mcap[df_with_mcap['Transaction'].str.lower() == 'sale']

                buy_df = buy_df.groupby(['Ticker', 'Transaction'], as_index=False).agg({
                'Value ($)': 'sum',
                'Value/Market Cap (%)': 'sum',
                'Owner': lambda x: ' & '.join(x[:5]),
                'Market Cap (B)': 'first'
                })

                sell_df = sell_df.groupby(['Ticker', 'Transaction'], as_index=False).agg({
                'Value ($)': 'sum',
                'Value/Market Cap (%)': 'sum',
                'Owner': lambda x: ' & '.join(x[:5]),
                'Market Cap (B)': 'first'
                })
            
                
                print(f"\nFound {len(buy_df)} buy transactions and {len(sell_df)} sale transactions")
                
                # Analyze the transactions to find top buys and sells
                top_buys = buy_df.sort_values('Value ($)', ascending=False).head(5) 
                top_sells = sell_df.sort_values('Value ($)', ascending=False).head(5)

        # Set display options to show all columns and wide display
                pd.set_option('display.max_columns', None)  # Show all columns
                pd.set_option('display.width', 1000)        # Set wide display width
                pd.set_option('display.expand_frame_repr', False)
                # Format numbers with commas for thousands and 3 decimal places
                pd.set_option('display.float_format', '{:,}'.format)

                # Display results with all columns
                print("\n=== TOP 5 LARGEST BUY TRANSACTIONS ===")
                print(top_buys)
                print("\n=== TOP 5 LARGEST SELL TRANSACTIONS ===")
                print(top_sells)


                
                # Also output top transactions by Value/Market Cap ratio
                print("\n=== TOP 5 LARGEST BUY TRANSACTIONS BY VALUE/MARKET CAP RATIO ===")
                top_buys_ratio = buy_df.sort_values('Value/Market Cap (%)', ascending=False).head(5)
                print(top_buys_ratio[['Ticker', 'Owner', 'Transaction', 'Value ($)', 'Market Cap (B)', 'Value/Market Cap (%)']])
                
                print("\n=== TOP 5 LARGEST SELL TRANSACTIONS BY VALUE/MARKET CAP RATIO ===")
                top_sells_ratio = sell_df.sort_values('Value/Market Cap (%)', ascending=False).head(5)
                print(top_sells_ratio[['Ticker', 'Owner', 'Transaction', 'Value ($)', 'Market Cap (B)', 'Value/Market Cap (%)']])
                
                # Save separate DataFrames to CSV files
                save_dataframe_to_csv(df_with_mcap, "insider_data_with_mcap.csv")
                save_dataframe_to_csv(buy_df, "insider_buys.csv")
                save_dataframe_to_csv(sell_df, "insider_sells.csv")
        else:
            # If no command line arguments, just list the tabs
            connect_to_chrome_and_list_tabs() 

        

    