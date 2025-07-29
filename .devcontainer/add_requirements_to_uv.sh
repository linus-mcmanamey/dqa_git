#!/bin/bash
# filepath: /workspaces/unify_2_1_dm_pipeline/add_requirements_to_uv.sh

# Function to display usage
usage() {
    echo "Usage: $0 [requirements_file]"
    echo "Default requirements file: .devcontainer/requirements.txt"
    exit 1
}

# Set default requirements file or use provided argument
REQUIREMENTS_FILE="${1:-/tmp/requirements.txt}"

# Check if requirements file exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "Error: Requirements file '$REQUIREMENTS_FILE' not found"
    exit 1
fi

echo "Adding packages from $REQUIREMENTS_FILE to uv..."

# Read requirements file line by line
while IFS= read -r line; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]]; then
        continue
    fi

    # Remove inline comments and trim whitespace
    package=$(echo "$line" | sed 's/#.*//' | xargs)

    # Skip if package is empty after processing
    if [[ -z "$package" ]]; then
        continue
    fi

    echo "Adding package: $package"

    # Add package to uv
    if uv pip install --system "$package"; then
        echo "Successfully install: $package"
    else
        echo "Failed to add: $package"
    fi

done < "$REQUIREMENTS_FILE"

echo "Finished processing requirements file"
