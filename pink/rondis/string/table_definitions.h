#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

/*
    NdbRecords are used for serialization. They map columns of a table to fields in a struct.
    For each table we interact with, we define:
    - one NdbRecord defining the columns to filter the row we want to read
    - one NdbRecord defining the columns we want to fetch
*/

/*
    KEY TABLE
*/

#define KEY_TABLE_NAME "redis_string_keys"
#define MAX_KEY_VALUE_LEN 3000
#define INLINE_VALUE_LEN 26500

int init_key_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_key_record;
extern NdbRecord *entire_key_record;

/*
    Doing this instead of reflection; Keep these the same
    as the field names in the key_table struct.
*/
#define KEY_TABLE_COL_redis_key "redis_key"
#define KEY_TABLE_COL_rondb_key "rondb_key"
#define KEY_TABLE_COL_expiry_date "expiry_date"
#define KEY_TABLE_COL_row_state "row_state"
#define KEY_TABLE_COL_tot_value_len "tot_value_len"
#define KEY_TABLE_COL_num_rows "num_rows"
#define KEY_TABLE_COL_value_start "value_start"

struct key_table
{
    Uint32 null_bits; // TODO: What's this for?
    char redis_key[MAX_KEY_VALUE_LEN + 2];
    Uint64 rondb_key;
    Uint32 expiry_date;
    Uint32 row_state;
    Uint32 tot_value_len;
    // Technically implicit
    Uint32 num_rows;
    char value_start[INLINE_VALUE_LEN + 2];
};

/*
    VALUE TABLE
*/

#define VALUE_TABLE_NAME "redis_string_values"
#define EXTENSION_VALUE_LEN 29500

int init_value_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_value_record;
extern NdbRecord *entire_value_record;

/*
    Doing this instead of reflection; Keep these the same
    as the field names in the value_table struct.
*/
#define VALUE_TABLE_COL_rondb_key "rondb_key"
#define VALUE_TABLE_COL_ordinal "ordinal"
#define VALUE_TABLE_COL_value "value"

struct value_table
{
    Uint64 rondb_key;
    Uint32 ordinal;
    char value[EXTENSION_VALUE_LEN];
};

/*
    EXPORT
*/

int init_string_records(NdbDictionary::Dictionary *dict);
