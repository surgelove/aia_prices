"""
main.py

Entrypoint for the price streaming service. This module exposes the
PriceStreamer class which manages a connection to Redis, reads
configuration and streams live prices from a broker implementation.

Run as a script to start a self-contained streamer process.
"""

import argparse
import json
import time
from zoneinfo import ZoneInfo
import redis
import threading
import uuid
import sys
import os
from pathlib import Path
import pandas as pd
import datetime
from zoneinfo import ZoneInfo

import broker
# import aia_utilities as au
import aia_utilities_test as au


def load_historical_data(credentials, instruments, rows=5000, granularity='S5'):
    """Fetch recent historical data for configured instruments and publish to Redis.

    It calls `broker.get_oanda_data` per instrument
    """
    print(f"📥 Loading historical data: rows={rows}, granularity={granularity}")

    tz = ZoneInfo("America/New_York")

    return_list = []
    print(f'{instruments=}')
    for instrument in instruments:
        print(f"📡 Fetching historical for {instrument}...")
        try:
            df = broker.get_oanda_data(credentials, instrument=instrument,
                                        granularity=granularity, rows=rows)
            if df is None or df.empty:
                print(f"⚠️ No historical data for {instrument}")
                continue

            for _, row in df.iterrows():
                # Use the DataFrame's timestamp and price/bid/ask columns returned by get_oanda_data
                timestamp = str(row['timestamp']) if 'timestamp' in row else None
                price = row['price'] if 'price' in row else None
                bid = row['bid'] if 'bid' in row else None
                ask = row['ask'] if 'ask' in row else None
                spread = ask - bid if bid is not None and ask is not None else None

                dt_local = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
                dt_utc = dt_local.astimezone(datetime.timezone.utc)
                timestamp_with_microseconds = dt_local.strftime("%Y-%m-%d %H:%M:%S.%f")

                # print(type(dt_local), type(dt_utc))
                # print(f"🕒 Local time: {dt_local}, UTC time: {dt_utc}")
                # print(f"📅 Timestamp: {timestamp_with_microseconds}")

                price_data = {
                    'timestamp': timestamp_with_microseconds,
                    'instrument': instrument,
                    'price': price,
                    'bid': bid,
                    'ask': ask,
                    'spread': spread
                }

                print(str(price_data)[:120]) 
                return_list.append(price_data)

        except Exception as e:
            print(f"❌ Error fetching historical for {instrument}: {e}")
    return return_list


def main():
    parser = argparse.ArgumentParser(description='Price Streamer')
    parser.add_argument('-b', '--broker', choices=['oanda', 'ib'],
                        default=None, help='Broker to use')
    parser.add_argument('-r', '--rows', type=int, default=2,
                        help='Number of historical rows to fetch per instrument')
    parser.add_argument('-t', '--ttl', type=int, default=14400,
                        help='TTL (seconds) for price messages and index (default 10)')
    parser.add_argument('-d', '--db', type=int, default=0,
                        help='Redis database number (default 0)')

    args = parser.parse_args()

    with open('config/secrets.json', 'r') as f:
        credentials = json.load(f)[args.broker]
    
    with open('config/main.json', 'r') as f:
        config = json.load(f)

        instruments = config.get('instruments', ['USD_CAD'])
        # active_instruments = set(instruments)
        redis_config = config.get('redis', {
            'host': 'localhost',
            'port': 6379,
            'db': 0
        })

    r = au.Redis_Utilities(host=redis_config.get('host', 'localhost'),
                            port=redis_config.get('port', 6379),
                            db=redis_config.get('db', 0))

    r.delete('prices')


    historical_data = load_historical_data(credentials, instruments, args.rows, 'S5')

    for row in historical_data:
        r.write('prices', row)

    tz = ZoneInfo("America/New_York")

    max_retries = 10
    retry_count = 0
    retry_delay = 5

    while True:
        # try:
        if 1==1:
            print(f"🔄 Starting/restarting OANDA price stream (attempt {retry_count + 1})")
            print(f"📊 Streaming instruments: {', '.join(instruments)}")

            # Create single stream for all active instruments
            instruments_list = list(instruments)
            if not instruments_list:
                print("⚠️ No active instruments, waiting...")
                time.sleep(5)
                continue
            
            # Convert list to comma-separated string for OANDA API
            instruments_string = ','.join(instruments_list)
            print(f"🔗 OANDA stream parameter: {instruments_string}")

            for price in broker.stream_oanda_live_prices(credentials, instruments_string):
                # Skip heartbeat messages
                if price.get('type') == 'HEARTBEAT' or 'heartbeat' in price:
                    continue
                
                instrument = price.get('instrument')
                
                # Skip if no instrument (likely a heartbeat or invalid message)
                if not instrument:
                    continue
                
                # Only process if instrument is still active
                if instrument not in instruments:
                    continue

                # Print price as it comes in
                # print(f"💰 {instrument}: {price['bid']} at {price['timestamp']}")

                # print('-----price-----')
                # print(price)
                # print('---------------')

                dt_local = datetime.datetime.strptime(str(price['timestamp']), "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=tz)
                dt_utc = dt_local.astimezone(datetime.timezone.utc)
                timestamp_with_microseconds = dt_local.strftime("%Y-%m-%d %H:%M:%S.%f")

                # print('price' + str(price))
                # Publish price data to Redis with TTL
                price_data = {
                    'timestamp': timestamp_with_microseconds,
                    'instrument': instrument,
                    'price': price['price'],
                    'bid': price['bid'],
                    'ask': price['ask'],
                    'spread_pips': price['spread_pips']
                }
                r.write('prices', price_data)
                print(str(price_data)[:120])

        # except Exception as e:
        #     retry_count += 1
        #     print(f"❌ Error in price stream: {e}")
        #     print(f"⏱️ Reconnecting in {retry_delay} seconds...")
    
            
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 1.5, 60)
    # streamer = PriceStreamer(args.broker, ttl=args.ttl)
    # streamer.run(rows=args.rows)

if __name__ == "__main__":
    main()

    

