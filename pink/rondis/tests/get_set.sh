#!/bin/bash

set -e

# Redis connection details (modify if needed)
REDIS_CLI="redis-cli"  # Adjust if redis-cli is not in your PATH
KEY="test_key"

# Function to set a value and retrieve it, then verify if it matches
function set_and_get() {
    local key="$1"
    local value="$2"
    
    # Set the value in Redis
    $REDIS_CLI SET "$key" "$value"
    
    # Retrieve the value
    local result=$($REDIS_CLI GET "$key")
    
    # Check if the retrieved value matches the expected value
    if [[ "$result" == "$value" ]]; then
        echo "PASS: $key with value length ${#value}"
    else
        echo "FAIL: $key with value length ${#value}"
        echo "Expected: $value"
        echo "Got: $result"
        exit 1
    fi
    echo
}

# Test Cases

echo "Testing empty string..."
set_and_get "$KEY:empty" ""

echo "Testing small string..."
set_and_get "$KEY:small" "hello"

echo "Testing medium string (100 characters)..."
medium_value=$(head -c 100 < /dev/zero | tr '\0' 'a')
set_and_get "$KEY:medium" "$medium_value"

echo "Testing large string (10,000 characters)..."
large_value=$(head -c 10000 < /dev/zero | tr '\0' 'a')
set_and_get "$KEY:large" "$large_value"

echo "Testing non-ASCII string..."
set_and_get "$KEY:nonascii" "こんにちは世界"  # Japanese for "Hello, World"

echo "Testing binary data..."
binary_value=$(echo -e "\x01\x02\x03\x04\x05\x06\x07")
set_and_get "$KEY:binary" "$binary_value"

echo "Testing unicode characters..."
unicode_value="🔥💧🌳"
set_and_get "$KEY:unicode" "$unicode_value"

echo "Testing multiple keys..."
for i in {1..10}; do
    test_value="Value_$i"_$(head -c $((RANDOM % 100 + 1)) < /dev/zero | tr '\0' 'a')
    set_and_get "$KEY:multiple_$i" "$test_value"
done

echo "Testing very large string (1,000,000 characters)..."
very_large_value=$(head -c 1000000 < /dev/zero | tr '\0' 'a')
set_and_get "$KEY:very_large" "$very_large_value"

echo "Testing edge case large key length (Redis allows up to 512MB for the value)..."
edge_value=$(head -c 100000 < /dev/zero | tr '\0' 'b')
set_and_get "$KEY:edge_large" "$edge_value"

echo "All tests completed."
