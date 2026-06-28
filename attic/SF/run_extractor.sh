#!/bin/bash

# Salesforce Post Extractor Runner
# This script helps you run the Selenium extractor for Salesforce cases

echo "🚀 Salesforce Post Extractor Setup"
echo "=================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found. Please install Python3."
    exit 1
fi

# Check if pip is available
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 not found. Please install pip3."
    exit 1
fi

# Install requirements if needed
echo "📦 Installing Python dependencies..."
pip3 install -r requirements.txt

# Check if Chrome is running in debug mode
echo ""
echo "🔍 Checking if Chrome is running in debug mode..."
if curl -s http://localhost:9222/json/version > /dev/null 2>&1; then
    echo "✅ Chrome debug mode detected on port 9222"
else
    echo "❌ Chrome debug mode not detected!"
    echo ""
    echo "📋 To start Chrome in debug mode, run:"
    echo "   google-chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-debug"
    echo ""
    echo "   Or on Windows:"
    echo "   chrome.exe --remote-debugging-port=9222"
    echo ""
    echo "Make sure to:"
    echo "1. Log into Salesforce in the Chrome browser"
    echo "2. Navigate to your Salesforce org"
    echo ""
    read -p "Press Enter after starting Chrome in debug mode..."
fi

# Check if Excel file exists
if [ ! -f "out/solved.xlsx" ]; then
    echo "❌ Excel file 'out/solved.xlsx' not found!"
    echo "Please make sure you have run the main script to generate the Excel file first."
    exit 1
fi

echo ""
echo "🎯 Ready to extract data from Salesforce cases!"
echo ""

# Run the extractor
python3 selenium_sf_extractor.py

echo ""
echo "✅ Extractor finished. Check the updated solved.xlsx file in the out/ directory."
