#!/bin/bash

# ===== Project Setup Script =====
# Usage: ./setup.sh

# Create venv
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python -m venv .venv
else
    echo "Virtual environment already exists."
fi

# Activate venv
source .venv/Scripts/activate 2>/dev/null || source .venv/bin/activate 2>/dev/null

# Install dependencies
echo "Installing/updating dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Setup complete."
