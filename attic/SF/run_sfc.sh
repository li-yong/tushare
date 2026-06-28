#!/bin/bash
# Wrapper script to run sfc.py with virtual environment

# Change to script directory
cd "$(dirname "$0")"

# Run the script with virtual environment
venv/bin/python sfc.py "$@"

# Show helpful message if Excel files were created
if [[ "$*" == *"--excel"* ]]; then
    echo ""
    echo "💡 Excel files contain:"
    echo "   • CaseNumber column with clickable links to Salesforce"
    echo "   • Subject column with cleaned subjects (if --clean-subjects was used)"
    echo "   • Open the .xlsx file in Excel, LibreOffice, or Google Sheets"
fi
