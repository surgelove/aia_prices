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


REDIS_HOST = "localhost"
REDIS_PORT = 6379

class PriceStreamer:
    def __init__(self, broker_name, ttl=None):
        assert broker_name in ['oanda', 'ib', 'alpaca'], "Invalid broker name"
        self.broker_name = broker_name
        self.credentials = None
        self.redis_client = None
        self.instruments = []
        self.active_instruments = set()
        self.price_key_prefix = "prices:"
        # Add TTL configuration
        self.price_ttl = 2  # Default 2 seconds
        # CLI override for TTL (if provided)
        self.cli_ttl = ttl
        self.load_credentials()
        self.load_config()
        # If CLI TTL provided, override config values to keep lifetime consistent
        if self.cli_ttl is not None:
            try:
                ttl_val = int(self.cli_ttl)
                self.price_ttl = ttl_val
                # historical TTL should match overall TTL unless user specifies otherwise in config
                self.hist_ttl = ttl_val
            except Exception:
                pass
        self.connect_to_redis()
        
    def format_timestamp(self, ts):
        """Return timestamp as 'YYYY-MM-DD HH:MM:SS.ffffff' (6 microsecond digits).

        Accepts datetime, pandas.Timestamp, numeric, or ISO string values and
        returns a consistently formatted string. Falls back to str(ts) on error.
        """
        if ts is None:
            return str(ts)
        try:
            # If it's already a string, try to parse reliably with pandas
            if isinstance(ts, str):
                parsed = pd.to_datetime(ts)
                return parsed.strftime('%Y-%m-%d %H:%M:%S.%f')
            # If it has strftime (datetime / pandas.Timestamp), use it
            if hasattr(ts, 'strftime'):
                return ts.strftime('%Y-%m-%d %H:%M:%S.%f')
            # Fallback to pandas conversion for other types
            parsed = pd.to_datetime(ts)
            return parsed.strftime('%Y-%m-%d %H:%M:%S.%f')
        except Exception:
            return str(ts)

    def load_config(self):
        """Load price streamer configuration."""
        try:
            with open('config/main.json', 'r') as f:
                config = json.load(f)
                self.instruments = config.get('instruments', ['USD_CAD'])
                self.active_instruments = set(self.instruments)
                self.redis_config = config.get('redis', {
                    'host': 'localhost',
                    'port': 6379,
                    'db': 0
                })
                # Load TTL configuration
                ttl_config = config.get('ttl', {})
                # Default to 10 seconds for price messages
                self.price_ttl = ttl_config.get('price_data', 10)  # Default 10 seconds
                # TTL for historical price messages (seconds). By default match live TTL
                self.hist_ttl = ttl_config.get('historical_price_data', 10)
            print(f"✅ Loaded config: tracking {len(self.instruments)} instruments")
            print(f"📊 Instruments: {', '.join(self.instruments)}")
            print(f"⏱️ TTL - Price data: {self.price_ttl}s")
        except FileNotFoundError:
            print("⚠️ No config/price.json found, using defaults")
            self.instruments = ['USD_CAD']
            self.active_instruments = set(self.instruments)
            self.redis_config = {'host': 'localhost', 'port': 6379, 'db': 0}
            # Keep default TTL values
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing config/price.json: {e}")
            raise
        
    def connect_to_redis(self):
        """Connect to Redis with retry logic."""
        max_redis_retries = 5
        redis_retry_delay = 2
        
        for attempt in range(max_redis_retries):
            try:
                self.redis_client = redis.Redis(
                    host=self.redis_config['host'], 
                    port=self.redis_config['port'], 
                    db=self.redis_config['db'], 
                    socket_connect_timeout=5
                )
                # Test connection
                self.redis_client.ping()
                
                # Disable Redis persistence to prevent dump.rdb file creation
                try:
                    self.redis_client.config_set('save', '')
                    print("✅ Disabled Redis persistence (no dump.rdb file)")
                except Exception as e:
                    print(f"⚠️ Could not disable Redis persistence: {e}")
                
                print("✅ Connected to Redis successfully")
                return
            except redis.ConnectionError as e:
                print(f"❌ Redis connection attempt {attempt + 1}/{max_redis_retries} failed: {e}")
                if attempt < max_redis_retries - 1:
                    print(f"⏱️ Retrying Redis connection in {redis_retry_delay} seconds...")
                    time.sleep(redis_retry_delay)
                else:
                    print("❌ Failed to connect to Redis after all attempts")
                    print("💡 Please ensure Redis is running: redis-server")
                    raise
        
    def add_instrument(self, instrument):
        """Add a new instrument to stream."""
        if instrument in self.active_instruments:
            print(f"⚠️ {instrument} is already being streamed")
        else:
            self.active_instruments.add(instrument)
            if instrument not in self.instruments:
                self.instruments.append(instrument)
            print(f"✅ Added {instrument} to active streaming")
        
        # Send confirmation back
        confirmation = {
            'status': 'success',
            'message': f'Added {instrument} to streaming',
            'active_instruments': list(self.active_instruments)
        }
        self.redis_client.lpush('price_streamer_responses', json.dumps(confirmation))

    def remove_instrument(self, instrument):
        """Remove an instrument from streaming."""
        if instrument not in self.active_instruments:
            print(f"⚠️ {instrument} is not currently being streamed")
        else:
            self.active_instruments.discard(instrument)
            print(f"🛑 Removed {instrument} from active streaming")
        
        # Send confirmation back
        confirmation = {
            'status': 'success',
            'message': f'Removed {instrument} from streaming',
            'active_instruments': list(self.active_instruments)
        }
        self.redis_client.lpush('price_streamer_responses', json.dumps(confirmation))

    def list_active_instruments(self):
        """List currently active instruments."""
        active = list(self.active_instruments)
        print(f"📊 Active instruments: {', '.join(active) if active else 'None'}")
        
        # Send response back
        response = {
            'status': 'success',
            'active_instruments': active,
            'instrument_count': len(self.active_instruments)
        }
        self.redis_client.lpush('price_streamer_responses', json.dumps(response))

    

    # Indexing feature removed: no cleanup helper required

    def run(self, rows=5000, granularity='S5'):
        """Start the price streaming service."""
        print(f"🚀 Starting self-contained price streaming service...")
        
        # Clear old price data on startup
        try:
            # Clean up any existing price keys
            old_keys = self.redis_client.keys(f"{self.price_key_prefix}*")
            if old_keys:
                self.redis_client.delete(*old_keys)
                print(f"🗑️ Cleared {len(old_keys)} old price keys")
            print("✨ Price data cleared and ready")
        except redis.ConnectionError as e:
            print(f"❌ Could not clear old price data: {e}")
        
        # Load historical data for configured instruments before starting live stream
        try:
            # Request configured number of historical rows per instrument
            self.load_historical_data(rows=rows, granularity=granularity)
        except Exception as e:
            print(f"⚠️ Could not load historical data: {e}")

        # Start price streaming
        price_thread = threading.Thread(target=self.run_price_stream, daemon=True)
        price_thread.start()
        
        print("✅ Price streaming service started")
        print("🎧 Ready for add/remove commands")
        print(f"⏰ Price messages auto-expire after {self.price_ttl} seconds")
        
        # Keep the main thread alive
        try:
                while True:
                    time.sleep(30)
                    # Count active price message keys by pattern
                    try:
                        keys = self.redis_client.keys(f"{self.price_key_prefix}*") if self.redis_client else []
                        active_count = len(keys)
                    except Exception:
                        active_count = 0
                    print(f"📈 Streaming {len(self.active_instruments)} instruments: {', '.join(self.active_instruments)}")
                    print(f"📊 Active price messages: {active_count}")
                        
        except KeyboardInterrupt:
            print("\n🛑 Price streamer stopped by user")



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

def run_price_stream(credentials, instruments):
    """Stream prices for all instruments using a single OANDA stream."""
    max_retries = 10
    retry_count = 0
    retry_delay = 5
    
    

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

    

