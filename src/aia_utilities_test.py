import json
import redis
import time
import uuid
from datetime import datetime
import subprocess
import threading
import pandas as pd
import pytz

class Redis_Utilities:
    """
    Utility class for interacting with Redis, including reading and writing JSON entries.
    """

    def __init__(self, host='localhost', port=6379, db=0, ttl=120, stream_maxlen=10000):
        """
        Initialize the Redis_Utilities instance.

        Args:
            host (str): Redis server host.
            port (int): Redis server port.
            db (int): Redis database number.
            ttl (int): Time-to-live for written keys in seconds.
        """
        self.host = host
        self.port = port
        self.db = db
        self.ttl = ttl
        self.redis_db = redis.Redis(host=self.host, port=self.port, db=self.db)
        # This utility uses Redis Streams (XADD/XREAD/XRANGE) exclusively.
        # stream_maxlen controls approximate trimming when writing.
        self.stream_maxlen = stream_maxlen

    def read_all(self, prefix, order=True):
        """
        Read all Redis entries matching the given prefix, returning them as a sorted list of dicts.

        Args:
            prefix (str): Key prefix to scan for.
            order (bool): If True, sort results by 'timestamp'.

        Returns:
            list: List of dicts sorted by 'timestamp'.
        """

        """
        Read all entries from the Redis Stream named `prefix` using XRANGE.

        Returns a list of dicts parsed from the stream 'data' field. If
        `order` is True and a 'timestamp' field exists it will sort by it
        (XRANGE already returns entries in ID order).
        """
        items = []
        try:
            entries = self.redis_db.xrange(prefix, min='-', max='+')
        except Exception as e:
            print(f"Error reading stream {prefix}: {e}")
            return items

        for entry_id, fields in entries:
            # fields can be {b'data': b'...'} or {'data': '...'} depending on client
            raw = fields.get(b'data') or fields.get('data')
            if raw is None:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"JSON decode error for stream entry {entry_id}: {e}")
                continue
            # attach id in case caller wants it
            data.setdefault('_id', entry_id)
            items.append(data)

        if order:
            try:
                items.sort(key=lambda x: x.get('timestamp', ''))
            except Exception:
                pass

        return items

    def read_each(self, prefix):
        """
        Continuously yield new Redis entries matching the given prefix as dicts.
        Prefix should be <prefix>:<instrument>

        Args:
            prefix (str): Key prefix to scan for.

        Yields:
            dict: Newly found Redis entry as a dict.
        """
        """
        Continuously yield new entries from the Redis Stream named `prefix`.

        This uses XREAD to block for new entries. It starts at '0-0' which
        will emit existing entries first; if you want only new entries use
        'last_id="$"' when calling this function externally and adapt as
        needed. Returned items are decoded JSON dicts.
        """
        last_id = '0-0'
        while True:
            try:
                # block for up to 1000ms waiting for new entries
                resp = self.redis_db.xread({prefix: last_id}, block=1000)
                if not resp:
                    continue
                for stream_key, entries in resp:
                    for entry_id, fields in entries:
                        raw = fields.get(b'data') or fields.get('data')
                        if raw is None:
                            continue
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error for stream entry {entry_id}: {e}")
                            continue
                        last_id = entry_id
                        yield data
            except Exception as e:
                print(f"Error reading stream {prefix}: {e}")
                time.sleep(0.5)

    def write(self, prefix, value):
        """
        Write a dict value to Redis with a generated key using the given prefix.
        Prefix should be <prefix>:<instrument>

        Args:
            prefix (str): Key prefix for the entry.
            value (dict): Value to store as JSON.
        """
        assert isinstance(value, dict)
        try:
            # store JSON in the 'data' field; approximate trimming to keep stream size bounded
            self.redis_db.xadd(prefix, {'data': json.dumps(value)}, maxlen=self.stream_maxlen, approximate=True)
        except Exception as e:
            print(f"Error writing to stream {prefix}: {e}")


    def show(self, prefix=None):
        """
        Inspect Redis and print keys and stream information.

        Args:
            prefix (str|None): Optional prefix to filter keys (pattern prefix*). If None lists all keys.

        Returns:
            list: List of dicts with key metadata (key, type, ttl, stream length, sample entries).
        """
        pattern = f"{prefix}*" if prefix else "*"
        results = []
        try:
            for key in self.redis_db.scan_iter(pattern):
                try:
                    key_str = key.decode() if isinstance(key, (bytes, bytearray)) else str(key)
                    t = self.redis_db.type(key)
                    if isinstance(t, bytes):
                        t = t.decode()
                    ttl = None
                    try:
                        ttl = self.redis_db.ttl(key)
                    except Exception:
                        pass

                    info = {"key": key_str, "type": t, "ttl": ttl}

                    if t == 'stream':
                        # stream metadata
                        try:
                            length = self.redis_db.xlen(key)
                        except Exception:
                            length = None
                        info['length'] = length
                        # also expose 'items' as a semantic alias for number of entries
                        info['items'] = length
                        # attempt to read first and last entries (best-effort)
                        try:
                            first = self.redis_db.xrange(key, min='-', max='+', count=1)
                            if first:
                                fid, ffields = first[0]
                                info['first_id'] = fid
                                raw = ffields.get(b'data') or ffields.get('data')
                                if isinstance(raw, bytes):
                                    raw = raw.decode()
                                info['first_entry'] = raw
                        except Exception:
                            pass
                        try:
                            # prefer xrevrange if available to get last entry
                            last = None
                            if hasattr(self.redis_db, 'xrevrange'):
                                lr = self.redis_db.xrevrange(key, max='+', min='-', count=1)
                                if lr:
                                    last = lr[0]
                            else:
                                # fallback: attempt to get last via xrange (not ideal for long streams)
                                lr = self.redis_db.xrange(key, min='-', max='+')
                                if lr:
                                    last = lr[-1]
                            if last:
                                lid, lfields = last
                                info['last_id'] = lid
                                raw = lfields.get(b'data') or lfields.get('data')
                                if isinstance(raw, bytes):
                                    raw = raw.decode()
                                info['last_entry'] = raw
                        except Exception:
                            pass

                    results.append(info)
                except Exception as e:
                    print(f"Error inspecting key {key}: {e}")
        except Exception as e:
            print(f"Error scanning keys with pattern {pattern}: {e}")

        # # pretty-print
        # for r in results:
        #     print(r)

        return results

    def delete(self, stream_name):
        """
        Delete a specific Redis stream key.

        Args:
            stream_name (str): The name of the stream to delete.

        Returns:
            bool: True if the key was deleted, False if it did not exist or on error.
        """
        try:
            res = self.redis_db.delete(stream_name)
            # redis.delete returns number of keys removed (0 or 1)
            return bool(res)
        except Exception as e:
            print(f"Error deleting stream {stream_name}: {e}")
            return False

    def trim(self, stream_name, timestamp):
        """
        Trim a stream by removing entries older than `timestamp` while keeping the
        stream key itself. This is a stub: expected behavior is to remove entries
        whose embedded `timestamp` field (or entry id if appropriate) is older
        than the provided timestamp. `timestamp` may be an ISO-8601 string or a
        datetime.

        Args:
            stream_name (str): Name of the Redis stream.
            timestamp (str|datetime): Cutoff; entries older than this should be removed.

        Returns:
            int|None: Number of removed entries or None if unimplemented/error.
        """
        # Normalize timestamp to datetime
        if isinstance(timestamp, str):
            dt = string_to_datetime(timestamp)
        elif isinstance(timestamp, datetime):
            dt = timestamp
        else:
            raise TypeError("timestamp must be an ISO string or datetime")

        if dt is None:
            print("Invalid timestamp provided")
            return None

        # Ensure UTC milliseconds
        try:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.UTC)
            else:
                dt = dt.astimezone(pytz.UTC)
        except Exception:
            # best-effort
            pass

        cutoff_ms = int(dt.timestamp() * 1000)
        minid = f"{cutoff_ms}-0"

        # Preferred fast path: use XTRIM MINID (server-side, single command)
        try:
            # XTRIM <key> MINID <minid>
            res = self.redis_db.execute_command("XTRIM", stream_name, "MINID", minid)
            # returns number of entries removed
            try:
                return int(res)
            except Exception:
                return res
        except Exception as e:
            # MINID may not be supported by server/driver; fallback to client-side batch delete
            print(f"XTRIM MINID not supported or failed: {e}; falling back to XRANGE+XDEL")

        # Fallback: XRANGE entries up to minid, then XDEL those entry IDs in one batch
        try:
            entries = self.redis_db.xrange(stream_name, min='-', max=minid)
            ids = [eid for eid, _ in entries]
            if not ids:
                return 0
            deleted = self.redis_db.xdel(stream_name, *ids)
            try:
                return int(deleted)
            except Exception:
                return deleted
        except Exception as e:
            print(f"Fallback trim failed: {e}")
            return None

class TimeBasedMovement:

    def __init__(self, range):
        """Create a TimeBasedMovement tracker.

        Args:
            range (int): Window in minutes to calculate movement over.
        """
        # Holds dicts: {'timestamp': <pd.Timestamp>, 'price': <float>}
        self.data = []
        self.range = range
        # Keep a reasonable maximum to avoid unbounded memory growth
        self.max_size = 500

    def add(self, timestamp, price):
        self.data.append(
            {
                "timestamp": timestamp,
                "price": price,
            }
        )
        
        # Remove oldest data if queue exceeds max size
        if len(self.data) > self.max_size:
            self.data.pop(0)

    def clear(self):
        """Reset the stored price history to empty."""
        # Simple reset to drop all stored points
        self.data = []

    def calc(self):
        # Calculate the movement of the price for the last 5 minutes
        if len(self.data) < 2:
            return 0.0

        # Get the price data for the last n minutes
        range_ago = self.data[-1]["timestamp"] - pd.Timedelta(minutes=self.range)
        relevant_data = [d for d in self.data if d["timestamp"] > range_ago]

        if not relevant_data:
            return 0.0

        # Calculate the price movement percentage
        start_price = relevant_data[0]["price"]
        end_price = relevant_data[-1]["price"]
        
        # Avoid division by zero
        if start_price == 0:
            return 0.0
            
        return ((end_price - start_price) / start_price) * 100
    
def string_to_datetime(date_string):
    """
    Convert a string to a datetime object.

    Args:
        date_string (str): The date string to convert.

    Returns:
        datetime: The converted datetime object or None on error.
    """
    try:
        return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    except Exception as e:
        print(f"Error converting string to datetime: {e}")
        return None

def datetime_to_string(dt):
    """
    Convert a datetime object to an ISO-8601 string.

    Args:
        dt (datetime): The datetime object to convert.

    Returns:
        str: The converted ISO-8601 string or None on error.
    """
    try:
        return dt.isoformat()
    except Exception as e:
        print(f"Error converting datetime to string: {e}")
        return None
    

def say_nonblocking(text, voice=None, volume=2):
    """Speak `text` using the macOS `say` command without blocking.

    The speaking is performed in a daemon thread so the caller can
    continue execution immediately. Volume is set with `osascript`.

    Args:
        text (str): The text to speak.
        voice (str, optional): Voice name to pass to `say`.
        volume (int, optional): Volume level (0-100). Default 2 here
            matches the historical value in the repo; users can change it.
    """
    print("Speaking:", text)
    def speak():
        try:
            # Set system volume before speaking
            # This requires 'osascript' which is available on macOS
            volume_cmd = ['osascript', '-e', f'set volume output volume {volume}']
            subprocess.run(volume_cmd, check=True)
            
            # Now speak the text
            cmd = ['say']
            if voice:
                cmd.extend(['-v', voice])
            cmd.append(text)
            subprocess.run(cmd, check=True)
            
            # Optional: Reset volume to a default level when done
            # subprocess.run(['osascript', '-e', 'set volume output volume 75'], check=True)
        except Exception as e:
            print(f"Error with text-to-speech: {e}")
    
    # Run in a separate thread to avoid blocking
    thread = threading.Thread(target=speak, daemon=True)
    thread.start()


def convert_utc_to_ny(utc_time_str):
    """Convert an ISO-8601 UTC timestamp (optionally ending with 'Z')
    into a naive datetime string in the America/New_York timezone.

    Returns a string formatted as YYYY-MM-DD HH:MM:SS or None on error.
    """
    try:
        return (datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
                .astimezone(pytz.timezone('America/New_York'))
                .replace(tzinfo=None, microsecond=0)
                .strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        print(f"Error converting time: {e}")
        return None

def updown(direction):
    """Return a human-friendly direction label for a numeric delta.

    Args:
        direction (float): Positive for up, negative for down, zero for neutral.

    Returns:
        str or None: 'up', 'down', or None for no movement.
    """
    return "up" if direction > 0 else "down" if direction < 0 else None




