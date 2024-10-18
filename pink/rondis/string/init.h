#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

extern NdbRecord *primary_redis_main_key_record;
extern NdbRecord *all_redis_main_key_record;
extern NdbRecord *primary_redis_key_value_record;
extern NdbRecord *all_redis_key_value_record;

#define INLINE_VALUE_LEN 26500
#define EXTENSION_VALUE_LEN 29500
#define MAX_KEY_VALUE_LEN 3000

struct redis_main_key
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

struct redis_key_value
{
    Uint64 key_id;
    Uint32 ordinal;
    char value[EXTENSION_VALUE_LEN];
};

int init_key_record_specs(NdbDictionary::Dictionary *dict);
int init_value_record_specs(NdbDictionary::Dictionary *dict);
