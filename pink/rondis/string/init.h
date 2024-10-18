#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#define INLINE_VALUE_LEN 26500
#define EXTENSION_VALUE_LEN 29500
#define MAX_KEY_VALUE_LEN 3000

#define KEY_TABLE_NAME "redis_string_keys"
#define VALUE_TABLE_NAME "redis_string_values"

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

struct value_table
{
    Uint64 key_id;
    Uint32 ordinal;
    char value[EXTENSION_VALUE_LEN];
};

extern NdbRecord *primary_redis_main_key_record;
extern NdbRecord *all_redis_main_key_record;
extern NdbRecord *primary_redis_key_value_record;
extern NdbRecord *all_redis_key_value_record;

int init_key_record_specs(NdbDictionary::Dictionary *dict);
int init_value_record_specs(NdbDictionary::Dictionary *dict);
