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

struct key_table
{
    Uint32 null_bits;
    char key_val[MAX_KEY_VALUE_LEN + 2];
    Uint64 key_id;
    Uint32 expiry_date;
    Uint32 tot_value_len;
    Uint32 num_rows;
    Uint32 row_state;
    Uint32 tot_key_len;
    char value[INLINE_VALUE_LEN + 2];
};

/*
    VALUE TABLE
*/

#define VALUE_TABLE_NAME "redis_string_values"
#define EXTENSION_VALUE_LEN 29500

int init_value_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_value_record;
extern NdbRecord *entire_value_record;

struct value_table
{
    Uint64 key_id;
    Uint32 ordinal;
    char value[EXTENSION_VALUE_LEN];
};
