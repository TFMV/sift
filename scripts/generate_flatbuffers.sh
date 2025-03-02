#!/bin/bash
set -e

# Ensure script is executed from the project root
cd "$(dirname "$0")/.."

# Check if flatc is installed
if ! command -v flatc &> /dev/null; then
    echo "Error: flatc is not installed"
    echo "Please install flatbuffers compiler from https://github.com/google/flatbuffers"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p pkg/schema/generated

# Generate Go code from schema
echo "Generating Go code from schema..."
flatc --go -o pkg/schema/generated/ pkg/schema/log_event.fbs

echo "Code generation complete!"
echo "Generated files are in pkg/schema/generated/" 