#!/bin/bash

set -e

# Change key suffix using script arguments
KEY_SUFFIX=${1:-0}
KEY="test_key_$KEY_SUFFIX"

# Function to set a value and retrieve it, then verify if it matches
function set_and_get() {
    local key="$1"
    local value="$2"
    
    # SET the value in Redis
    if [[ -f "$value" ]]; then
        set_output=$(redis-cli --pipe <<EOF
SET $key $(< "$value")
EOF
)
    else
        set_output=$(redis-cli SET "$key" "$value")
    fi

    echo $set_output
    if [[ $set_output == ERR* ]]; then
        echo "FAIL: Could not SET $key with given value"
        exit 1
    fi
    
    # GET the value
    local result=$(redis-cli GET "$key")

    local expected_hash=$(echo -n "$value" | sha256sum | awk '{print $1}')
    local actual_hash=$(echo -n "$result" | sha256sum | awk '{print $1}')
    
    # Check if the retrieved value matches the expected value
    if [[ "$expected_hash" == "$actual_hash" ]]; then
        echo "PASS: $key with value length ${#value}"
    else
        echo "FAIL: $key with value length ${#value}; got length ${#result}"
        echo "Expected hash:    $expected_hash"
        echo "Received hash:    $actual_hash"
        exit 1
    fi
    echo
}

generate_random_chars() {
  local length=$1
  local random_string=""

  while [ "${#random_string}" -lt "$length" ]; do
    random_string+=$(head /dev/urandom | LC_CTYPE=C tr -dc 'a-zA-Z0-9' | head -c "$length")
  done

  echo "${random_string:0:$length}"
}

# Test Cases

echo "Testing ping..."
redis-cli ping && echo

echo "Testing empty string..."
set_and_get "$KEY:empty" ""

echo "Testing small string..."
set_and_get "$KEY:small" "hello"

# Minimal amount to create value rows: 30000
for NUM_CHARS in 100 10000 30000 50000 57000 60000 70000; do
    echo "Testing string with $NUM_CHARS characters..."
    test_value=$(generate_random_chars $NUM_CHARS)
    set_and_get "$KEY:$NUM_CHARS" "$test_value"
done

# echo "Testing xxl string (1,000,000 characters)..."
# xxl_file=$(mktemp)
# head -c 1000000 < /dev/zero | tr '\0' 'a' > "$xxl_file"
# set_and_get "$KEY:xxl" "$xxl_file"
# rm "$xxl_file"

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

# echo "Testing edge case large key length (Redis allows up to 512MB for the value)..."
# edge_value=$(head -c 100000 < /dev/zero | tr '\0' 'b')
# set_and_get "$KEY:edge_large" "$edge_value"

echo "All tests completed."
