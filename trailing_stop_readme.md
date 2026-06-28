# Futu Trailing Stop Loss

This feature automatically implements a trailing stop loss strategy for positions in your Futu trading account.

## What is a Trailing Stop Loss?

A trailing stop loss is a type of stop loss order that "trails" the market price by a fixed percentage when the market moves in your favor. Unlike a fixed stop loss that is set at a specific price, a trailing stop loss adjusts upward as the price rises, but stays fixed if the price falls, providing a mechanism to:

1. Lock in profits as a stock's price rises
2. Exit the position automatically if the price falls by your specified percentage from its highest point

## Features

- Monitors positions in your Futu account across US, HK, and A-share markets
- Automatically tracks the highest price reached for each position
- Sets a stop loss at a configurable percentage below the highest price
- Automatically sells positions when the stop price is triggered
- Persists highest price information between runs
- Supports both simulation and real account trading
- Can be limited to specific stocks in your portfolio

## Usage

Use the convenient shell script `run_trailing_stop.sh` to run the trailing stop loss feature:

```bash
# Run with default settings (5% trailing stop in simulation mode)
./run_trailing_stop.sh

# Use an 8% trailing stop
./run_trailing_stop.sh -p 8

# Monitor specific stocks only
./run_trailing_stop.sh -c US.AAPL,HK.00700,SH.600519

# Run in real account mode (use with caution!)
./run_trailing_stop.sh -r
```

### Command Line Options

- `-h, --help`: Show help message
- `-d, --debug`: Enable debug mode
- `-r, --real`: Use real trading account (default: simulation)
- `-p, --percent VALUE`: Set trailing stop percentage (default: 5%)
- `-c, --codes LIST`: Comma-separated list of stock codes to monitor
- `--host HOST`: Set FutuOpenD host (default: 127.0.0.1)
- `--port PORT`: Set FutuOpenD port (default: 11111)
- `--config FILE`: Set config file path (default: trailing_stop_config.json)

## Requirements

- FutuOpenD must be running and accessible
- Trading account must be logged in to FutuOpenD
- Python 3.6+ with required dependencies
- The Futu API libraries must be installed

## Implementation Details

The trailing stop loss monitors the price of positions at regular intervals and:

1. Tracks the highest price reached for each position
2. Calculates the stop price as (highest_price * (1 - trail_percent/100))
3. Places a market sell order if the current price drops to or below the stop price
4. Sets price reminders in the Futu app when new stop prices are calculated

## Limitations

- The monitoring runs while the script is active - it should ideally be run on a continuously available machine
- Stop loss orders are executed as market orders, which may result in slippage during volatile market conditions
- Trading is subject to the hours of the respective exchanges and the quotation permissions of your Futu account

## Caution

When using the real account mode (`-r` flag), actual trades will be placed with real money. Always test in simulation mode first and understand the risks involved in automated trading strategies. 