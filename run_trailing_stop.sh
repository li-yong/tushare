#!/bin/bash

# Default settings
HOST="127.0.0.1"
PORT="11111"
TRAIL_PERCENT=5
CONFIG_FILE="trailing_stop_config.json"
DEBUG=false
REAL_ACCOUNT=false
SPECIFIC_CODES=""

# Display help message
function show_help {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -h, --help            Show this help message"
    echo "  -d, --debug           Enable debug mode"
    echo "  -r, --real            Use real trading account (default: simulation)"
    echo "  -p, --percent VALUE   Set trailing stop percentage (default: 5%)"
    echo "  -c, --codes LIST      Comma-separated list of stock codes to monitor"
    echo "  --host HOST           Set FutuOpenD host (default: 127.0.0.1)"
    echo "  --port PORT           Set FutuOpenD port (default: 11111)"
    echo "  --config FILE         Set config file path (default: trailing_stop_config.json)"
    echo ""
    echo "Examples:"
    echo "  $0 -p 8                     # 8% trailing stop loss in simulation mode"
    echo "  $0 -r -p 5                  # 5% trailing stop loss in real account"
    echo "  $0 -c US.AAPL,HK.00700      # Monitor specific stocks only"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            show_help
            ;;
        -d|--debug)
            DEBUG=true
            shift
            ;;
        -r|--real)
            REAL_ACCOUNT=true
            shift
            ;;
        -p|--percent)
            TRAIL_PERCENT="$2"
            shift 2
            ;;
        -c|--codes)
            SPECIFIC_CODES="$2"
            shift 2
            ;;
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            ;;
    esac
done

# Build command
CMD="python t_futu_trade.py --trailing_stop --trail_percent $TRAIL_PERCENT --trail_config $CONFIG_FILE --host $HOST --port $PORT"

# Add real account flag if needed
if [ "$REAL_ACCOUNT" = true ]; then
    echo "WARNING: Running in REAL ACCOUNT mode!"
    echo "Press CTRL+C to cancel, or Enter to continue..."
    read
    CMD="$CMD --real_account"
fi

# Add debug flag if needed
if [ "$DEBUG" = true ]; then
    CMD="$CMD --debug"
fi

# Add specific codes if provided
if [ -n "$SPECIFIC_CODES" ]; then
    CMD="$CMD --trail_codes $SPECIFIC_CODES"
fi

# Display and execute the command
echo "Executing: $CMD"
echo "Press CTRL+C to stop the trailing stop loss monitoring"
eval $CMD 