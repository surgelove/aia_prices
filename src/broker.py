"""
broker.py

Utilities for connecting to OANDA price services.

This module provides helpers to stream live prices from OANDA, query
instrument precision and fetch historical candle data. Functions are
kept lightweight so they can be reused by other parts of the project.

Note: These functions call the live OANDA endpoints. Ensure credentials
are set in `config/secrets.json` and that you understand connecting to
live market data before running.
"""

import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import pytz
import json
import math
import sys
from pathlib import Path

import helper

def stream_oanda_live_prices(credentials, instrument='USD_CAD', callback=None, max_duration=None):
    """
    Stream live prices from OANDA API for a single instrument
    
    Parameters:
    -----------
    credentials : dict
        Dictionary containing 'api_key' and 'account_id'
    instrument : str, default='USD_CAD'
        Single instrument to stream (e.g., 'EUR_USD', 'GBP_JPY')
    callback : function, optional
        Function to call with each price update: callback(timestamp, instrument, bid, ask, price)
    max_duration : int, optional
        Maximum streaming duration in seconds (None = unlimited)
    
    Returns:
    --------
            # Append a normalized row for the DataFrame. Use mid prices
            # for OHLC so the DataFrame represents a consistent view.
        Yields price dictionaries or None if connection fails
    """
    
    api_key = credentials.get('api_key')
    account_id = credentials.get('account_id')
    
    # LIVE OANDA STREAMING API URL
    STREAM_URL = "https://stream-fxtrade.oanda.com"
    
    print("🔴 CONNECTING TO OANDA LIVE STREAMING")
    print("=" * 50)
    print("⚠️  WARNING: This connects to LIVE market stream")
    print(f"📊 Streaming: {instrument}")
    print(f"⏱️  Duration: {'Unlimited' if max_duration is None else f'{max_duration}s'}")
    print("=" * 50)
    
    # Validate inputs
    if not api_key:
        print("❌ ERROR: Live API key is required!")
        return None
    
    if not account_id:
        print("❌ ERROR: Live Account ID is required!")
        return None
    
    # Headers for streaming request
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/stream+json',
        'Content-Type': 'application/json'
    }
    
    # Streaming endpoint for prices - FIXED URL CONSTRUCTION
    stream_url = f"{STREAM_URL}/v3/accounts/{account_id}/pricing/stream"
    
    # Parameters for streaming
    params = {
        'instruments': instrument,
        'snapshot': 'true'  # Include initial snapshot
    }
    
    try:
        print(f"🌐 Initiating streaming connection...")
        print(f"   URL: {stream_url}")
        print(f"   Instrument: {instrument}")
        
        # Get instrument precision - FIXED: Use the BASE API URL, not streaming URL
        BASE_API_URL = "https://api-fxtrade.oanda.com"
        precision = get_instrument_precision(credentials, instrument)
        if precision is None:
            precision = 5  # Default precision
            print(f"⚠️  Using default precision: {precision}")

        # Make streaming request
        response = requests.get(stream_url, headers=headers, params=params, stream=True, timeout=30)
        
        # Check for HTTP errors
        if response.status_code == 401:
            print("❌ AUTHENTICATION ERROR (401)")
            print("   • Check your API key is correct")
            print("   • Ensure your API key has streaming permissions")
            return None
        elif response.status_code == 403:
            print("❌ FORBIDDEN ERROR (403)")
            print("   • Your account may not have streaming access")
            print("   • Check if your account is verified and funded")
            return None
        elif response.status_code == 404:
            print(f"❌ NOT FOUND ERROR (404)")
            print(f"   • Check instrument name: {instrument}")
            print(f"   • URL used: {stream_url}")
            return None
        elif response.status_code != 200:
            print(f"❌ HTTP ERROR {response.status_code}")
            print(f"   Response: {response.text}")
            return None
        
        print("✅ Streaming connection established!")
        print("📈 Receiving live price updates...")
        print("   Press Ctrl+C to stop streaming")
        print("-" * 50)
        
        start_time = time.time()
        price_count = 0
        previous_price = None
        
        # Process streaming data line by line
        for line in response.iter_lines():
            # Check duration limit
            if max_duration and (time.time() - start_time) > max_duration:
                print(f"\n⏰ Reached maximum duration of {max_duration} seconds")
                break
                
            if line:
                try:
                    # Parse JSON data
                    data = json.loads(line.decode('utf-8'))
                    
                    # Handle different types of messages
                    if data.get('type') == 'PRICE':
                        timestamp = datetime.now()
                        
                        # Extract price information
                        instrument_name = data.get('instrument', instrument)
                        
                        # Get bid/ask prices
                        bids = data.get('bids', [])
                        asks = data.get('asks', [])
                        
                        if bids and asks:
                            bid_price = round(float(bids[0]['price']), precision)
                            ask_price = round(float(asks[0]['price']), precision)   
                            mid_price = round((bid_price + ask_price) / 2, precision)
                            spread_pips = (ask_price - bid_price) * 10000  # For most pairs
                            
                            # Skip if price hasn't changed
                            if previous_price is not None and bid_price == previous_price:
                                continue
                            
                            # Update previous price
                            previous_price = bid_price
                            price_count += 1
                            
                            # Create price dictionary
                            price_data = {
                                'timestamp': timestamp,
                                'instrument': instrument_name,
                                'bid': bid_price,
                                'ask': ask_price,
                                'price': mid_price,  # Mid price for compatibility
                                'spread_pips': round(spread_pips, 1),
                                'time': data.get('time', timestamp.isoformat()),
                                'tradeable': data.get('tradeable', True)
                            }
                            
                            # # Print price update (every 10th update to avoid spam)
                            # if price_count % 10 == 0:
                            #     print(f"💰 {timestamp.strftime('%H:%M:%S')} | {instrument_name} | "
                            #           f"Bid: {bid_price:.5f} | Ask: {ask_price:.5f} | "
                            #           f"Mid: {mid_price:.5f} | Spread: {spread_pips:.1f} pips")
                            
                            # Call callback function if provided
                            if callback:
                                try:
                                    callback(timestamp, instrument_name, bid_price, ask_price, mid_price)
                                except Exception as e:
                                    print(f"⚠️  Callback error: {e}")
                            
                            # Yield price data for generator usage
                            yield price_data
                            
                    elif data.get('type') == 'HEARTBEAT':
                        # Heartbeat to keep connection alive
                        if price_count % 100 == 0:  # Print occasionally
                            print(f"💓 Heartbeat - Connection alive ({price_count} prices received)")
                    
                    else:
                        # Other message types
                        print(f"📨 Message: {data.get('type', 'Unknown')} - {data}")
                        
                except json.JSONDecodeError as e:
                    print(f"⚠️  JSON decode error: {e}")
                    continue
                except Exception as e:
                    print(f"⚠️  Processing error: {e}")
                    continue
        
        print(f"\n✅ Streaming completed. Total prices received: {price_count}")
        
    except KeyboardInterrupt:
        print(f"\n🛑 Streaming stopped by user. Total prices received: {price_count}")
    except requests.exceptions.Timeout:
        print("❌ TIMEOUT ERROR: Streaming request timed out")
    except requests.exceptions.ConnectionError:
        print("❌ CONNECTION ERROR: Lost connection to OANDA")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")
    finally:
        if 'response' in locals():
            response.close()

def get_instrument_precision(credentials, instrument_name):

    """
    Retrieves the display precision (decimal places) for a financial instrument from OANDA.

    Makes an authenticated request to the OANDA API to get instrument details and 
    returns the number of decimal places used for price display.

    Args:
        credentials (dict): Dictionary containing 'api_key' and 'account_id'
        instrument_name (str): The instrument name (e.g., 'EUR_USD', 'USD_CAD').

    Returns:
        int: Number of decimal places for price display, or None if instrument not found or error occurs.
    """
    
    api_key = credentials.get('api_key')
    account_id = credentials.get('account_id')
    url = "https://api-fxtrade.oanda.com"

    endpoint = f"{url}/v3/accounts/{account_id}/instruments"
    headers = {'Authorization': f'Bearer {api_key}'}
    params = None
    data = None
    
    try:
        response = requests.get(endpoint, headers=headers, params=params, data=data)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        instruments = response.json()['instruments']
        
        for instrument in instruments:
            if instrument['name'] == instrument_name:
                return instrument['displayPrecision']
                
        return None # Instrument not found
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def get_oanda_data(credentials, instrument='USD_CAD', granularity='S5', hours=5, rows=5000):
    """
    Connect to OANDA live fxtrade environment and fetch real market data
    
    Parameters:
    -----------
    credentials : dict
        Dictionary containing 'api_key' and 'account_id'
    instrument : str, default='USD_CAD'
        Currency pair to fetch
    granularity : str, default='S5'
        Time granularity (S5 = 5 seconds)
    hours : int, default=10
        Number of hours of historical data to fetch
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with real market data
    """
    
    api_key = credentials.get('api_key')
    account_id = credentials.get('account_id')
    
    # LIVE OANDA API URL (NOT practice!)
    BASE_URL = "https://api-fxtrade.oanda.com"
    
    print("🔴 CONNECTING TO OANDA LIVE FXTRADE ENVIRONMENT")
    print("=" * 55)
    print("⚠️  WARNING: This will connect to LIVE market data")
    print(f"📊 Requesting: {instrument} | {granularity} | Last {hours} hours")
    print("=" * 55)
    
    # Validate inputs
    if not api_key or api_key == "your_live_api_key_here":
        print("❌ ERROR: Live API key is required!")
        print("\n🔧 TO GET YOUR LIVE OANDA CREDENTIALS:")
        print("1. Log into your OANDA account at: https://www.oanda.com/")
        print("2. Go to 'Manage API Access' in account settings")
        print("3. Generate a Personal Access Token")
        print("4. Copy your Account ID from account overview")
        print("\n💡 USAGE:")
        print("live_data = connect_oanda_live({")
        print("    'api_key': 'your_actual_api_key',")
        print("    'account_id': 'your_actual_account_id'")
        print("})")
        return None
    
    if not account_id or account_id == "your_live_account_id_here":
        print("❌ ERROR: Live Account ID is required!")
        return None
    
    # Headers for API request
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Calculate count based on granularity and hours
    if granularity == 'S5':
        count = min(hours * 60 * 12, rows)  # 12 five-second intervals per minute, max 5000
    elif granularity == 'S10':
        count = min(hours * 60 * 6, rows)   # 6 ten-second intervals per minute
    elif granularity == 'M1':
        count = min(hours * 60, rows)       # 60 one-minute intervals per hour
    elif granularity == 'M5':
        count = min(hours * 12, rows)       # 12 five-minute intervals per hour
    else:
        count = min(7200, rows)  # Default fallback

    # API endpoint for historical candles
    url = f"{BASE_URL}/v3/instruments/{instrument}/candles"
    
    # Parameters for the request - explicitly ask for Bid+Ask series (BA)
    params = {
        'count': count,
        'granularity': granularity,
        # Request both Bid and Ask price series so we can return real bid/ask
        'price': 'BA'
        # Note: do not set 'includeFirst' unless using a 'from' timestamp on a candle boundary.
    }
    
    try:
        print(f"🌐 Making API request to OANDA live servers...")
        print(f"   URL: {url}")
        print(f"   Params: {params}")
        
        # Make the API request
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        # Check for HTTP errors
        if response.status_code == 401:
            print("❌ AUTHENTICATION ERROR (401)")
            print("   • Check your API key is correct")
            print("   • Ensure your API key has proper permissions")
            print("   • Verify you're using the live account API key")
            return None
        elif response.status_code == 403:
            print("❌ FORBIDDEN ERROR (403)")
            print("   • Your account may not have API access enabled")
            print("   • Check if your account is verified and funded")
            return None
        elif response.status_code == 404:
            print("❌ NOT FOUND ERROR (404)")
            print(f"   • Check instrument name: {instrument}")
            print(f"   • Check granularity: {granularity}")
            return None
        elif response.status_code != 200:
            print(f"❌ HTTP ERROR {response.status_code}")
            print(f"   Response: {response.text}")
            return None
        
        # Parse JSON response
        data = response.json()
        print('-----here')
        print(data)

        
        if 'candles' not in data:
            print("❌ ERROR: No candles data in response")
            print(f"Response: {data}")
            return None
        
        candles = data['candles']
        print(f"✅ Successfully received {len(candles)} candles from OANDA live")
        
        # Convert to DataFrame
        market_data = []
        for candle in candles:
            # Convert timestamp to New York timezone and remove timezone info
            timestamp = pd.to_datetime(candle['time'])
            # Convert to New York timezone
            timestamp = timestamp.tz_convert('America/New_York')
            # Remove timezone info (localize to None)
            timestamp = timestamp.tz_localize(None)
            
            # Extract OHLC data if available; OANDA may return 'mid', or only 'bid' and 'ask'.
            mid = candle.get('mid')
            bid = candle.get('bid')
            ask = candle.get('ask')

            # helper to safely parse floats
            def sf(obj, key):
                try:
                    if obj is None:
                        return None
                    v = obj.get(key)
                    if v is None:
                        return None
                    return float(v)
                except Exception:
                    return None

            # If mid exists, prefer it for OHLC. Otherwise, build mid-like OHLC from bid/ask averages.
            if mid:
                open_price = sf(mid, 'o')
                high_price = sf(mid, 'h')
                low_price = sf(mid, 'l')
                close_price = sf(mid, 'c')
            else:
                # Average bid/ask where possible
                open_b = sf(bid, 'o')
                open_a = sf(ask, 'o')
                high_b = sf(bid, 'h')
                high_a = sf(ask, 'h')
                low_b = sf(bid, 'l')
                low_a = sf(ask, 'l')
                close_b = sf(bid, 'c')
                close_a = sf(ask, 'c')

                def avg(a, b):
                    if a is None and b is None:
                        return None
                    if a is None:
                        return b
                    if b is None:
                        return a
                    return (a + b) / 2.0

                open_price = avg(open_b, open_a)
                high_price = avg(high_b, high_a)
                low_price = avg(low_b, low_a)
                close_price = avg(close_b, close_a)

            # Determine bid/ask close prices, falling back to close_price when missing
            bid_price = sf(bid, 'c') if bid else None
            ask_price = sf(ask, 'c') if ask else None
            if bid_price is None and close_price is not None:
                bid_price = close_price
            if ask_price is None and close_price is not None:
                ask_price = close_price
            
            # Calculate spread in pips (for USD/CAD, 1 pip = 0.0001)
            spread_pips = (ask_price - bid_price) * 10000
            
            market_data.append({
                'timestamp': timestamp,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'mid': close_price,
                'bid': bid_price,
                'ask': ask_price,
                'volume': candle.get('volume', 0),
                'spread_pips': round(spread_pips, 1),
                'complete': candle.get('complete', True)
            })
        
        if not market_data:
            print("❌ ERROR: No valid market data received")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(market_data)
        
        # Add price column for compatibility with EMA functions
        df['price'] = df['close']
        
        # Sort by timestamp to ensure chronological order
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        print(f"\n📊 LIVE MARKET DATA SUMMARY:")
        print(f"   • Instrument: {instrument}")
        print(f"   • Granularity: {granularity}")
        print(f"   • Total candles: {len(df):,}")
        print(f"   • Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"   • Price range: {df['close'].min():.5f} - {df['close'].max():.5f}")
        print(f"   • Current price: {df['close'].iloc[-1]:.5f}")
        print(f"   • Average spread: {df['spread_pips'].mean():.1f} pips")
        

        # # Show latest data (disabled by default)
        # print(f"\n📈 LATEST 3 CANDLES:")
        # latest_cols = ['timestamp', 'open', 'high', 'low', 'close', 'bid', 'ask', 'spread_pips']
        # print(df[latest_cols].tail(3).to_string(index=False, float_format='%.5f'))

        # return the dataframe including bid and ask columns so callers
        # can publish true historical bid/ask values
        return df[['timestamp', 'price', 'bid', 'ask']]

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return None

def get_transactions(credentials, hours_ago):

    # Get time hours ago based on new york time
    start_time = datetime.now(pytz.timezone('America/New_York')) - timedelta(hours=hours_ago)
    end_time = datetime.now(pytz.timezone('America/New_York')) + timedelta(hours=5)

    from_time = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    to_time = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    print(from_time)
    print(to_time)

    api_key = credentials.get('api_key')
    account_id = credentials.get('account_id')
    url = "https://api-fxtrade.oanda.com"

    endpoint = f"/v3/accounts/{account_id}/transactions"
    headers = {'Authorization': f'Bearer {api_key}'}
    params = {'from': from_time, 'to': to_time}
    data = None

    response = requests.get(f"{url}{endpoint}", headers=headers, params=params).json()

    return_transactions = []
    for page in response.get('pages', []):
        transactions = requests.get(page, headers=headers).json().get('transactions', [])
        for transaction in transactions:
            trx_type = transaction.get('type')
            units = transaction.get('units', '')
            wording = f'{trx_type}, Units: {units}, Reason: {transaction.get("reason", "N/A")}'
            price = transaction.get('price', None)
            if str(price) == 'nan':
                price = None
            if price: 
                price = float(price)
                if math.isnan(price):
                    price = None
            print(price)

            return_transactions.append({
                'timestamp': helper.convert_utc_to_ny(transaction.get('time')),
                # 'price': transaction.get('price'),
                'trx_price': price,
                'trx_type': trx_type
                # 'details': wording

                # 'id': transaction.get('id'),
                # 'accountID': transaction.get('accountID'),
                # 'userID': transaction.get('userID'),
                # 'batchID': transaction.get('batchID'),
                # 'requestID': transaction.get('requestID'),
                # 'time': convert_utc_to_ny(transaction.get('time')),
                # 'instrument': transaction.get('instrument'),
                # 'units': transaction.get('units'),
                # 'timeInForce': transaction.get('timeInForce'),
                # 'gtdTime': transaction.get('gtdTime'),
                # 'triggerCondition': transaction.get('triggerCondition'),
                # 'partialFill': transaction.get('partialFill'),
                # 'positionFill': transaction.get('positionFill'),
                # 'stopLossOnFill': transaction.get('stopLossOnFill'),
                # 'trailingStopLossOnFill': transaction.get('trailingStopLossOnFill'),
                # 'reason': transaction.get('reason'),
            })
    return return_transactions