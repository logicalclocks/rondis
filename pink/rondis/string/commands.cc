#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "db_interactions.h"
#include "../common.h"
#include "table_definitions.h"

/*
    A successful GET will return in this format:
        $5
        Hello
    where:
    - $ indicates a Bulk String reply
    - 5 is the length of the value ("Hello" has 5 characters)
    - Hello is the actual value stored at the key
    Special cases:
        $-1
    The key does not exist.
        $0
    The key exists but has no value (empty string).
*/
void rondb_get_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response)
{
    const char *key_str = argv[1].c_str();
    Uint32 key_len = argv[1].size();
    if (key_len > MAX_KEY_VALUE_LEN)
    {
        assign_generic_err_to_response(response, REDIS_KEY_TOO_LARGE);
        return;
    }

    const NdbDictionary::Dictionary *dict = ndb->getDictionary();
    const NdbDictionary::Table *tab = dict->getTable(KEY_TABLE_NAME);
    if (tab == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TABLE_OBJECT,
                                   dict->getNdbError().code);
        return;
    }

    struct key_table key_row;
    // varbinary -> first 2 bytes are length if bigger than 255
    // start copying from 3rd byte
    memcpy(&key_row.redis_key[2], key_str, key_len);
    // Length as little endian
    key_row.redis_key[0] = key_len & 255;
    key_row.redis_key[1] = key_len >> 8;
    int ret_code = get_simple_key_row(response, tab, ndb, &key_row, key_len);
    if ((ret_code != 0) || (&key_row)->num_rows == 0)
    {
        return;
    }
    else
    {
        /*
            Our value uses value rows, so a more complex read is required.
            We're starting from scratch here since we'll use a shared lock
            on the key table this time we read from it.
        */
        get_complex_key_row(response,
                            dict,
                            tab,
                            ndb,
                            &key_row,
                            key_len);
        return;
    }
}

void rondb_set_command(
    Ndb *ndb,
    const pink::RedisCmdArgsType &argv,
    std::string *response)
{
    Uint32 key_len = argv[1].size();
    if (key_len > MAX_KEY_VALUE_LEN)
    {
        assign_generic_err_to_response(response, REDIS_KEY_TOO_LARGE);
        return;
    }

    const char *key_str = argv[1].c_str();
    const char *value_str = argv[2].c_str();
    Uint32 value_len = argv[2].size();

    const NdbDictionary::Dictionary *dict = ndb->getDictionary();
    if (dict == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_DICT,
                                   dict->getNdbError().code);
        return;
    }
    const NdbDictionary::Table *tab = dict->getTable(KEY_TABLE_NAME);
    if (tab == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TABLE_OBJECT,
                                   dict->getNdbError().code);
        return;
    }

    NdbTransaction *trans = ndb->startTransaction(tab, key_str, key_len);
    if (trans == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TXN_OBJECT,
                                   ndb->getNdbError().code);
        return;
    }
    char varsize_param[EXTENSION_VALUE_LEN + 500];
    Uint32 value_rows = 0;
    Uint64 key_id = 0;
    if (value_len > INLINE_VALUE_LEN)
    {
        /**
         * The row doesn't fit in one RonDB row, create more rows
         * in the value_tables table.
         *
         * We also use the generated key_id which is the foreign
         * key column in the key_table table such that
         * deleting the row in the main table ensures that all
         * value rows are also deleted.
         */
        int ret_code = rondb_get_key_id(tab, key_id, ndb, response);
        if (ret_code == -1)
        {
            return;
        }
        Uint32 remaining_len = value_len - INLINE_VALUE_LEN;
        const char *start_value_ptr = &value_str[INLINE_VALUE_LEN];
        do
        {
            Uint32 this_value_len = remaining_len;
            if (remaining_len > EXTENSION_VALUE_LEN)
            {
                this_value_len = EXTENSION_VALUE_LEN;
            }
            int ret_code = create_value_row(response,
                                            ndb,
                                            dict,
                                            trans,
                                            start_value_ptr,
                                            key_id,
                                            this_value_len,
                                            value_rows,
                                            &varsize_param[0]);
            if (ret_code == -1)
            {
                return;
            }
            remaining_len -= this_value_len;
            start_value_ptr += this_value_len;
            if (((value_rows & 1) == 1) || (remaining_len == 0))
            {
                if (execute_no_commit(trans, ret_code, false) != 0)
                {
                    assign_ndb_err_to_response(response,
                                               FAILED_EXEC_TXN,
                                               ret_code);
                    return;
                }
            }
            value_rows++;
        } while (remaining_len > 0);
        value_len = INLINE_VALUE_LEN;
    }
    {
        int ret_code = create_key_row(response,
                                      ndb,
                                      tab,
                                      trans,
                                      key_id,
                                      key_str,
                                      key_len,
                                      value_str,
                                      value_len,
                                      Uint32(0),
                                      value_rows,
                                      Uint32(0),
                                      &varsize_param[0]);
        if (ret_code == -1)
        {
            return;
        }
    }
    response->append("+OK\r\n");
    return;
}
