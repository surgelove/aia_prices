# Human instructions

Connects to brokers and streams prices on a redis stream. As part of Aia.

ttl defaults to 14400 (60 secs * 60 minutes * 4 hours)
rows of history defaults to 5000
broker is mandatory
```bash
python3 src/main.py --broker oanda --rows 1000 --ttl 2
```