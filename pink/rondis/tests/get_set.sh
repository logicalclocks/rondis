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

echo "Testing ping..."
redis-cli ping && echo

echo "Testing empty string..."
set_and_get "$KEY:empty" ""

echo "Testing small string..."
set_and_get "$KEY:small" "hello"

echo "Testing medium string (100 characters)..."
medium_value=$(head -c 100 < /dev/zero | tr '\0' 'a')
set_and_get "$KEY:medium" "$medium_value"

# Too large values seem to fail due to the network buffer size
# Minimal amount to create value rows: 30000

# TODO: Increase this as soon as GH actions allows it
echo "Testing large string (10,000 characters)..."
large_value=$(head -c 10000 < /dev/zero | tr '\0' 'a')
set_and_get "$KEY:large" "$large_value"

# echo "Testing xl string (100,000 characters)..."
# xl_value=$(head -c 100000 < /dev/zero | tr '\0' 'a')
# set_and_get "$KEY:xl" "$xl_value"

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
