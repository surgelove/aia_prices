"""
main.py

Entrypoint for the price streaming service. This module exposes the
PriceStreamer class which manages a connection to Redis, reads
configuration and streams live prices from a broker implementation.

Run as a script to start a self-contained streamer process.
"""

import json
import time
import redis
import threading
import uuid
import sys
import os
from pathlib import Path
import pandas as pd

import broker

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
        
    def load_credentials(self):
        """Load OANDA credentials."""
        try:
            with open('config/secrets.json', 'r') as f:
                self.credentials = json.load(f)[self.broker_name]
            print("✅ Loaded OANDA credentials successfully")
        except FileNotFoundError:
            print("❌ Error: secrets.json not found in config directory")
            raise
        except json.JSONDecodeError:
            print("❌ Error: Invalid JSON in secrets.json")
            raise
        print(self.credentials)

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

    def run_price_stream(self):
        """Stream prices for all instruments using a single OANDA stream."""
        max_retries = 10
        retry_count = 0
        retry_delay = 5
        
        while True:
            try:
                print(f"🔄 Starting/restarting OANDA price stream (attempt {retry_count + 1})")
                print(f"📊 Streaming instruments: {', '.join(self.active_instruments)}")
                
                # Create single stream for all active instruments
                instruments_list = list(self.active_instruments)
                if not instruments_list:
                    print("⚠️ No active instruments, waiting...")
                    time.sleep(5)
                    continue
                
                # Convert list to comma-separated string for OANDA API
                instruments_string = ','.join(instruments_list)
                print(f"🔗 OANDA stream parameter: {instruments_string}")

                for price in broker.stream_oanda_live_prices(self.credentials, instruments_string):
                    # Skip heartbeat messages
                    if price.get('type') == 'HEARTBEAT' or 'heartbeat' in price:
                        continue
                    
                    instrument = price.get('instrument')
                    
                    # Skip if no instrument (likely a heartbeat or invalid message)
                    if not instrument:
                        continue
                    
                    # Only process if instrument is still active
                    if instrument not in self.active_instruments:
                        continue

                    # Print price as it comes in
                    print(f"💰 {instrument}: {price['bid']} at {price['timestamp']}")
                        
                    # Publish price data to Redis with TTL
                    price_data = {
                        'timestamp': price['timestamp'].isoformat() if hasattr(price['timestamp'], 'isoformat') else str(price['timestamp']),
                        'instrument': instrument,
                        'price': price['price'],
                        'bid': price['bid'],
                        'ask': price['ask'],
                        'spread_pips': price['spread_pips']
                    }
                    
                    try:
                        # Generate unique key for this price message
                        price_key = f"{self.price_key_prefix}{instrument}:{uuid.uuid4().hex[:8]}"
                        
                        # Set the price data with configurable TTL
                        self.redis_client.setex(price_key, self.price_ttl, json.dumps(price_data))
                    except redis.ConnectionError:
                        print(f"❌ Lost Redis connection, attempting to reconnect...")
                        self.connect_to_redis()
                        
                        # Retry the operation: simply store the price key
                        price_key = f"{self.price_key_prefix}{instrument}:{uuid.uuid4().hex[:8]}"
                        self.redis_client.setex(price_key, self.price_ttl, json.dumps(price_data))
                        print(f"📤 Put {instrument} after reconnect")

                    retry_count = 0
                    retry_delay = 5
                    
                print(f"⚠️ Price stream ended. Attempting to reconnect...")
                retry_count += 1
                
            except Exception as e:
                retry_count += 1
                print(f"❌ Error in price stream: {e}")
                print(f"⏱️ Reconnecting in {retry_delay} seconds...")
            
            if max_retries > 0 and retry_count >= max_retries:
                print(f"❌ Failed to connect after {max_retries} attempts. Giving up.")
                break
                
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 1.5, 60)

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

    def load_historical_data(self, rows=5000, granularity='S5'):
        """Fetch recent historical data for configured instruments and publish to Redis.

        It calls `broker.get_oanda_data` per instrument and writes each candle
        as a short-lived price message into Redis so downstream consumers
        immediately have recent context.
        """
        print(f"📥 Loading historical data: rows={rows}, granularity={granularity}")

        for instrument in list(self.active_instruments):
            print(f"📡 Fetching historical for {instrument}...")
            try:
                df = broker.get_oanda_data(self.credentials, instrument=instrument,
                                           granularity=granularity, rows=rows)
                if df is None or df.empty:
                    print(f"⚠️ No historical data for {instrument}")
                    continue

                published = 0
                for _, row in df.iterrows():
                    # Use the DataFrame's timestamp and price/bid/ask columns returned by get_oanda_data
                    ts_val = row['timestamp'] if 'timestamp' in row else None
                    price = row['price'] if 'price' in row else None
                    bid = row['bid'] if 'bid' in row else None
                    ask = row['ask'] if 'ask' in row else None
                    # Normalize price: convert pandas NA to None and ensure numeric
                    if pd.isna(price):
                        price = None
                    else:
                        try:
                            price = float(price)
                        except Exception:
                            price = None

                    # Coerce bid/ask/price to floats when possible, prefer explicit bid/ask
                    def to_float_safe(x):
                        try:
                            if pd.isna(x):
                                return None
                            return float(x)
                        except Exception:
                            return None

                    bid = to_float_safe(bid)
                    ask = to_float_safe(ask)
                    price = to_float_safe(price)

                    # If explicit bid/ask are missing, fall back to mid/price
                    if bid is None and price is not None:
                        bid = price
                    if ask is None and price is not None:
                        ask = price

                    spread_pips = None
                    if bid is not None and ask is not None:
                        spread_pips = round((ask - bid) * 10000, 1)

                    # Prepare timestamp for storage (ISO) and for display (match live print)
                    if ts_val is None or pd.isna(ts_val):
                        ts_iso = str(ts_val)
                        display_ts = str(ts_val)
                    else:
                        # ISO string for storage
                        try:
                            ts_iso = ts_val.isoformat() if hasattr(ts_val, 'isoformat') else str(ts_val)
                        except Exception:
                            ts_iso = str(ts_val)

                        # Live printing uses datetime.__str__ (space-separated). Produce same display.
                        try:
                            if isinstance(ts_val, str):
                                # Convert '2025-08-25T12:34:56' -> '2025-08-25 12:34:56'
                                display_ts = ts_val.replace('T', ' ').replace('Z', '')
                            else:
                                display_ts = str(ts_val)
                        except Exception:
                            display_ts = str(ts_val)

                    price_data = {
                        'timestamp': ts_iso,
                        'bid': bid,
                        'ask': ask,
                        'price': price,
                        'spread_pips': spread_pips,
                        'instrument': instrument
                    }

                    # Print historical data in the same format as live updates
                    # Print using the same layout as live updates with formatting
                    if price_data['bid'] is None:
                        print(f"💰 {instrument}: <no-bid> at {display_ts}")
                    else:
                        try:
                            print(f"💰 {instrument}: {price_data['bid']:.5f} at {display_ts}")
                        except Exception:
                            print(f"💰 {instrument}: {price_data['bid']} at {display_ts}")

                    try:
                        price_key = f"{self.price_key_prefix}{instrument}:{uuid.uuid4().hex[:8]}"
                        # Use configured historical TTL (defaults to 10s)
                        hist_ttl = self.hist_ttl

                        # Debug: print exactly what we are about to publish to Redis
                        try:
                            publish_json = json.dumps(price_data, ensure_ascii=False)
                        except Exception:
                            publish_json = str(price_data)
                        print(f"▶ PUBLISH -> {price_key} : {publish_json}")

                        # Store the historical message
                        self.redis_client.setex(price_key, hist_ttl, publish_json)

                        # Verify the key exists before considering it published
                        # (avoid counting keys that failed to write)
                        try:
                            stored = self.redis_client.get(price_key)
                        except Exception:
                            stored = None
                        if stored is not None:
                            published += 1
                            # Print a small sample for debugging
                            try:
                                stub = stored.decode('utf-8') if isinstance(stored, (bytes, bytearray)) else str(stored)
                                print(f"🔍 Stored sample for {price_key}: {stub[:160]}")
                            except Exception:
                                pass
                        else:
                            print(f"❗ Stored value missing for {price_key} after SETEX (not indexing)")
                    except redis.ConnectionError:
                        print(f"❌ Lost Redis connection while publishing historical {instrument}")
                        self.connect_to_redis()
                        break

                print(f"✅ Published {published} historical messages for {instrument}")
            except Exception as e:
                print(f"❌ Error fetching historical for {instrument}: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Price Streamer')
    parser.add_argument('-b', '--broker', choices=['oanda', 'ib', 'alpaca'],
                        default='oanda', help='Broker to use')
    parser.add_argument('-r', '--rows', type=int, default=5000,
                        help='Number of historical rows to fetch per instrument')
    parser.add_argument('-t', '--ttl', type=int, default=10,
                        help='TTL (seconds) for price messages and index (default 10)')
    
    args = parser.parse_args()
    
    streamer = PriceStreamer(args.broker, ttl=args.ttl)
    streamer.run(rows=args.rows)
