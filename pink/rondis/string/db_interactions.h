#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

int create_key_row(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Table *tab,
                   NdbTransaction *trans,
                   Uint64 key_id,
                   const char *key_str,
                   Uint32 key_len,
                   const char *value_str,
                   Uint32 value_len,
                   Uint32 field_rows,
                   Uint32 value_rows,
                   Uint32 row_state,
                   char *buf);

int create_value_row(std::string *response,
                         Ndb *ndb,
                         const NdbDictionary::Dictionary *dict,
                         NdbTransaction *trans,
                         const char *start_value_ptr,
                         Uint64 key_id,
                         Uint32 this_value_len,
                         Uint32 ordinal,
                         char *buf);

/*
    Since the beginning of the value is saved within the key table, it
    can suffice to read the key table to get the value. If the value is
*/
int get_simple_key_row(std::string *response,
                       const NdbDictionary::Table *tab,
                       Ndb *ndb,
                       struct key_table *row,
                       Uint32 key_len);

int get_complex_key_row(std::string *response,
                        const NdbDictionary::Dictionary *dict,
                        const NdbDictionary::Table *tab,
                        Ndb *ndb,
                        struct key_table *row,
                        Uint32 key_len);

int get_value_rows(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Dictionary *dict,
                   NdbTransaction *trans,
                   const Uint32 num_rows,
                   const Uint64 key_id,
                   const Uint32 this_value_len,
                   const Uint32 tot_value_len);

int rondb_get_key_id(const NdbDictionary::Table *tab,
                     Uint64 &key_id,
                     Ndb *ndb,
                     std::string *response);
