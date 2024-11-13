#!/bin/bash

set -e

# Change key suffix using script arguments
HASH_KEY="key"
KEY_SUFFIX=${1:-0}
KEY="test_key_$KEY_SUFFIX"

# Function to set a value and retrieve it, then verify if it matches
function hset_and_hget() {
    local field="$1"
    local value="$2"
    
    # SET the value in Redis
    if [[ -f "$value" ]]; then
        set_output=$(redis-cli --pipe <<EOF
HSET $HASH_KEY $field $(< "$value")
EOF
)
    else
        set_output=$(redis-cli HSET "$HASH_KEY" "$field" "$value")
    fi

    #echo $set_output
    if [[ $set_output == ERR* ]]; then
        echo "FAIL: Could not SET $field with given value"
        exit 1
    fi
    
    # GET the value
    local result=$(redis-cli HGET "$HASH_KEY" "$field")

    local expected_hash=$(echo -n "$value" | sha256sum | awk '{print $1}')
    local actual_hash=$(echo -n "$result" | sha256sum | awk '{print $1}')
    
    # Check if the retrieved value matches the expected value
    if [[ "$expected_hash" == "$actual_hash" ]]; then
        echo "PASS: $field with value length ${#value}"
    else
        echo "FAIL: $field with value length ${#value}; got length ${#result}"
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
hset_and_hget "$KEY:empty" ""

echo "Testing small string..."
hset_and_hget "$KEY:small" "hello"

# Minimal amount to create value rows: 30000
for NUM_CHARS in 100 10000 30000 50000 57000 60000 70000; do
    echo "Testing string with $NUM_CHARS characters..."
    test_value=$(generate_random_chars $NUM_CHARS)
    hset_and_hget "$KEY:$NUM_CHARS" "$test_value"
done

# echo "Testing xxl string (1,000,000 characters)..."
# xxl_file=$(mktemp)
# head -c 1000000 < /dev/zero | tr '\0' 'a' > "$xxl_file"
# set_and_get "$KEY:xxl" "$xxl_file"
# rm "$xxl_file"

echo "Testing non-ASCII string..."
hset_and_hget "$KEY:nonascii" "こんにちは世界"  # Japanese for "Hello, World"

echo "Testing binary data..."
binary_value=$(echo -e "\x01\x02\x03\x04\x05\x06\x07")
hset_and_hget "$KEY:binary" "$binary_value"

echo "Testing unicode characters..."
unicode_value="🔥💧🌳"
hset_and_hget "$KEY:unicode" "$unicode_value"

echo "Testing multiple keys..."
for i in {1..10}; do
    test_value="Value_$i"_$(head -c $((RANDOM % 100 + 1)) < /dev/zero | tr '\0' 'a')
    hset_and_hget "$KEY:multiple_$i" "$test_value"
done

echo "Testing piped keys..."
for i in {1..10000}; do
    echo "HSET $HASH_KEY $KEY:piped_$i value_$i"
done | redis-cli --pipe --verbose

echo "Testing edge case large key length (Redis allows up to 512MB for the value)..."
edge_value=$(head -c 100000 < /dev/zero | tr '\0' 'b')
hset_and_hget "$KEY:edge_large" "$edge_value"

field="key"
incr_field="$field:incr$RANDOM"
incr_output=$(redis-cli HINCR "$HASH_KEY" "$incr_field")
incr_result=$(redis-cli HGET "$HASH_KEY" "$incr_field")
if [[ "$incr_result" == 1 ]]; then
    echo "PASS: Incrementing non-existing key $incr_field "
else
    echo "FAIL: Incrementing non-existing key $incr_field"
    echo "Expected: 1"
    echo "Received: $incr_result"
    exit 1
fi

incr_start_value=$RANDOM
hset_and_hget "$incr_field" $incr_start_value
for i in {1..10}; do
    incr_output=$(redis-cli HINCR "$HASH_KEY" "$incr_field")
    incr_result=$(redis-cli HGET "$HASH_KEY" "$incr_field")
    incr_expected_value=$((incr_start_value + i))
    if [[ "$incr_result" == $incr_expected_value ]]; then
        echo "PASS: Incrementing field $incr_field to value $incr_result"
    else
        echo "FAIL: Incrementing field $incr_field from value $incr_start_value"
        echo "Expected: $incr_expected_value"
        echo "Received: $incr_result"
        exit 1
    fi
done

echo "All tests completed."
