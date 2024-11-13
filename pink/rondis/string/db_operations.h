#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#ifndef STRING_DB_OPERATIONS_H
#define STRING_DB_OPERATIONS_H

const Uint32 ROWS_PER_READ = 2;

int create_key_row(std::string *response,
                   const NdbDictionary::Table *tab,
                   NdbTransaction *trans,
                   Uint64 redis_key_id,
                   Uint64 rondb_key,
                   const char *key_str,
                   Uint32 key_len,
                   const char *value_str,
                   Uint32 tot_value_len,
                   Uint32 num_value_rows,
                   Uint32 &prev_num_rows,
                   Uint32 row_state);

int write_data_to_key_op(std::string *response,
                         const NdbOperation **ndb_op,
                         const NdbDictionary::Table *tab,
                         NdbTransaction *trans,
                         Uint64 redis_key_id,
                         Uint64 rondb_key,
                         const char *key_str,
                         Uint32 key_len,
                         const char *value_str,
                         Uint32 tot_value_len,
                         Uint32 num_value_rows,
                         Uint32 &prev_num_rows,
                         Uint32 row_state,
                         NdbRecAttr **recAttr);

int delete_key_row(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Table *tab,
                   NdbTransaction *trans,
                   Uint64 redis_key_id,
                   const char *key_str,
                   Uint32 key_len,
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

int create_all_value_rows(std::string *response,
                          Ndb *ndb,
                          const NdbDictionary::Dictionary *dict,
                          NdbTransaction *trans,
                          Uint64 rondb_key,
                          const char *value_str,
                          Uint32 value_len,
                          Uint32 num_value_rows,
                          char *buf);

int delete_value_rows(std::string *response,
                      const NdbDictionary::Table *tab,
                      NdbTransaction *trans,
                      Uint64 rondb_key,
                      Uint32 start_ordinal,
                      Uint32 end_ordinal);
/*
    Since the beginning of the value is saved within the key table, it
    can suffice to read the key table to get the value. If the value is
*/
int get_simple_key_row(std::string *response,
                       const NdbDictionary::Table *tab,
                       Ndb *ndb,
                       NdbTransaction *trans,
                       struct key_table *key_row);

int get_complex_key_row(std::string *response,
                        const NdbDictionary::Dictionary *dict,
                        const NdbDictionary::Table *tab,
                        Ndb *ndb,
                        NdbTransaction *trans,
                        struct key_table *row);

int get_value_rows(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Dictionary *dict,
                   NdbTransaction *trans,
                   const Uint32 num_rows,
                   const Uint64 key_id,
                   const Uint32 tot_value_len);

int read_batched_value_rows(std::string *response,
                            NdbTransaction *trans,
                            const Uint64 rondb_key,
                            const Uint32 num_rows_to_read,
                            const Uint32 start_ordinal,
                            const NdbTransaction::ExecType commit_type);

int rondb_get_rondb_key(const NdbDictionary::Table *tab,
                        Uint64 &key_id,
                        Ndb *ndb,
                        std::string *response);

void incr_key_row(std::string *response,
                  Ndb *ndb,
                  const NdbDictionary::Table *tab,
                  NdbTransaction *trans,
                  struct key_table *key_row);

int rondb_get_redis_key_id(Ndb *ndb,
                           Uint64 &redis_key_id,
                           const char *key_str,
                           Uint32 key_len,
                           std::string *response);
#endif
