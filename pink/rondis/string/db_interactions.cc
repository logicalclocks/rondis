#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "../common.h"
#include "table_definitions.h"

NdbRecord *pk_key_record = nullptr;
NdbRecord *entire_key_record = nullptr;
NdbRecord *pk_value_record = nullptr;
NdbRecord *entire_value_record = nullptr;

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
                   char *buf)
{
    NdbOperation *write_op = trans->getNdbOperation(tab);
    if (write_op == nullptr)
    {
        ndb->closeTransaction(trans);
        failed_get_operation(response);
        return -1;
    }
    write_op->writeTuple();

    memcpy(&buf[2], key_str, key_len);
    buf[0] = key_len & 255;
    buf[1] = key_len >> 8;
    write_op->equal("key_val", buf);

    if (key_id == 0)
    {
        write_op->setValue("key_id", (char *)NULL);
    }
    else
    {
        write_op->setValue("key_id", key_id);
    }
    write_op->setValue("tot_value_len", value_len);
    write_op->setValue("num_rows", value_rows);
    write_op->setValue("tot_key_len", key_len);
    write_op->setValue("row_state", row_state);
    write_op->setValue("expiry_date", 0);

    if (value_len > INLINE_VALUE_LEN)
    {
        value_len = INLINE_VALUE_LEN;
    }
    memcpy(&buf[2], value_str, value_len);
    buf[0] = value_len & 255;
    buf[1] = value_len >> 8;
    write_op->setValue("value", buf);
    {
        int ret_code = write_op->getNdbError().code;
        if (ret_code != 0)
        {
            ndb->closeTransaction(trans);
            failed_define(response, ret_code);
            return -1;
        }
    }
    {
        int ret_code = 0;
        if (((value_rows == 0) &&
             (execute_commit(ndb, trans, ret_code) == 0)) ||
            (execute_no_commit(trans, ret_code, true) == 0))
        {
            return 0;
        }
        int write_op_error = write_op->getNdbError().code;
        if (write_op_error != FOREIGN_KEY_RESTRICT_ERROR)
        {
            ndb->closeTransaction(trans);
            failed_execute(response, ret_code);
            return -1;
        }
    }
    /**
     * There is a row that we need to overwrite and this row
     * also have value rows. Start by deleting the key row,
     * this will lead to deletion of all value rows as well.
     *
     * If new row had no value rows the transaction will already
     * be aborted and need to restarted again.
     *
     * After deleting the key row we are now ready to insert the
     * key row.
     */
    if (value_rows == 0)
    {
        ndb->closeTransaction(trans);
        ndb->startTransaction(tab, key_str, key_len);
        if (trans == nullptr)
        {
            failed_create_transaction(response, ndb->getNdbError().code);
            return -1;
        }
    }
    {
        NdbOperation *del_op = trans->getNdbOperation(tab);
        if (del_op == nullptr)
        {
            ndb->closeTransaction(trans);
            failed_get_operation(response);
            return -1;
        }
        del_op->deleteTuple();
        del_op->equal("key_val", buf);
        {
            int ret_code = del_op->getNdbError().code;
            if (ret_code != 0)
            {
                ndb->closeTransaction(trans);
                failed_define(response, ret_code);
                return -1;
            }
        }
    }
    {
        int ret_code = 0;
        if (execute_no_commit(trans, ret_code, false) == -1)
        {
            ndb->closeTransaction(trans);
            failed_execute(response, ret_code);
            return -1;
        }
    }
    {
        NdbOperation *insert_op = trans->getNdbOperation(tab);
        if (insert_op == nullptr)
        {
            ndb->closeTransaction(trans);
            failed_get_operation(response);
            return -1;
        }
        insert_op->insertTuple();
        insert_op->equal("key_val", buf);
        insert_op->setValue("tot_value_len", value_len);
        insert_op->setValue("value_rows", value_rows);
        insert_op->setValue("tot_key_len", key_len);
        insert_op->setValue("row_state", row_state);
        insert_op->setValue("expiry_date", 0);
        {
            int ret_code = insert_op->getNdbError().code;
            if (ret_code != 0)
            {
                ndb->closeTransaction(trans);
                failed_define(response, ret_code);
                return -1;
            }
        }
    }
    {
        int ret_code = 0;
        if (execute_commit(ndb, trans, ret_code) == 0)
        {
            return 0;
        }
        ndb->closeTransaction(trans);
        failed_execute(response, ret_code);
        return -1;
    }
}

int create_value_row(std::string *response,
                     Ndb *ndb,
                     const NdbDictionary::Dictionary *dict,
                     NdbTransaction *trans,
                     const char *start_value_ptr,
                     Uint64 key_id,
                     Uint32 this_value_len,
                     Uint32 ordinal,
                     char *buf)
{
    const NdbDictionary::Table *tab = dict->getTable(VALUE_TABLE_NAME);
    if (tab == nullptr)
    {
        failed_create_table(response, ndb->getNdbError().code);
        ndb->closeTransaction(trans);
        return -1;
    }
    NdbOperation *op = trans->getNdbOperation(tab);
    if (op == nullptr)
    {
        ndb->closeTransaction(trans);
        failed_get_operation(response);
        return -1;
    }
    op->insertTuple();
    op->equal("key_id", key_id);
    op->equal("ordinal", ordinal);
    memcpy(&buf[2], start_value_ptr, this_value_len);
    buf[0] = this_value_len & 255;
    buf[1] = this_value_len >> 8;
    op->equal("value", buf);
    {
        int ret_code = op->getNdbError().code;
        if (ret_code != 0)
        {
            ndb->closeTransaction(trans);
            failed_define(response, ret_code);
            return -1;
        }
    }
    return 0;
}

int get_simple_key_row(std::string *response,
                       const NdbDictionary::Table *tab,
                       Ndb *ndb,
                       struct key_table *key_row,
                       Uint32 key_len)
{
    // This is (usually) a local operation to calculate the correct data node, using the
    // hash of the pk value.
    NdbTransaction *trans = ndb->startTransaction(tab,
                                                  &key_row->key_val[0],
                                                  key_len + 2);
    if (trans == nullptr)
    {
        failed_create_transaction(response, ndb->getNdbError().code);
        return RONDB_INTERNAL_ERROR;
    }
    /**
     * Mask and options means simply reading all columns
     * except primary key column.
     */

    const Uint32 mask = 0xFE;
    const unsigned char *mask_ptr = (const unsigned char *)&mask;
    const NdbOperation *read_op = trans->readTuple(
        pk_key_record,
        (const char *)key_row,
        entire_key_record,
        (char *)key_row,
        NdbOperation::LM_CommittedRead,
        mask_ptr);
    if (read_op == nullptr)
    {
        ndb->closeTransaction(trans);
        failed_get_operation(response);
        return RONDB_INTERNAL_ERROR;
    }
    if (trans->execute(NdbTransaction::Commit,
                       NdbOperation::AbortOnError) != 0 ||
        read_op->getNdbError().code != 0)
    {
        /*
            // TODO: Replace hard-coded error numbers with generic error handling
            //     The NDB API does not supply these error codes itself so it would be guess-work

            NdbError::Status status = read_op->getNdbError().status;
            if (status == NdbError::Success)
            {
                return 0;
            }
            else if (status == NdbError::PermanentError)
            {
                failed_no_such_row_error(response);
                return READ_ERROR;
            }
        */

        int ret_code = read_op->getNdbError().code;
        if (ret_code == READ_ERROR)
        {
            failed_no_such_row_error(response);
            return READ_ERROR;
        }
        failed_read_error(response, ret_code);
        return RONDB_INTERNAL_ERROR;
    }

    if (key_row->num_rows > 0)
    {
        return 0;
    }
    char buf[20];
    int len = write_formatted(buf,
                              sizeof(buf),
                              "$%u\r\n",
                              key_row->tot_value_len);
    response->reserve(key_row->tot_value_len + len + 3);
    response->append(buf);
    response->append((const char *)&key_row->value[2], key_row->tot_value_len);
    response->append("\r\n");
    printf("Respond with len: %d, %u tot_value_len, string: %s, string_len: %u\n",
           len,
           key_row->tot_value_len,
           response->c_str(),
           Uint32(response->length()));
    ndb->closeTransaction(trans);
    return 0;
}

int get_value_rows(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Dictionary *dict,
                   NdbTransaction *trans,
                   const Uint32 num_rows,
                   const Uint64 key_id,
                   const Uint32 tot_value_len)
{
    const NdbDictionary::Table *tab = dict->getTable(VALUE_TABLE_NAME);
    if (tab == nullptr)
    {
        failed_create_table(response, ndb->getNdbError().code);
        ndb->closeTransaction(trans);
        response->clear();
        return -1;
    }

    // Break up fetching large values to avoid blocking the network for other reads
    const int ROWS_PER_READ = 2;
    struct value_table value_rows[ROWS_PER_READ];

    for (Uint32 row_index = 0; row_index < num_rows; row_index++)
    {
        int read_index = row_index % ROWS_PER_READ;
        value_rows[read_index].key_id = key_id;
        value_rows[read_index].ordinal = row_index;

        bool is_last_row_of_read = (read_index == (ROWS_PER_READ - 1));
        bool is_last_row = (row_index == (num_rows - 1));
        if (!is_last_row_of_read && !is_last_row)
        {
            continue;
        }

        const NdbOperation *read_op = trans->readTuple(
            pk_value_record,
            (const char *)&value_rows,
            entire_value_record,
            (char *)&value_rows,
            NdbOperation::LM_CommittedRead);
        if (read_op == nullptr)
        {
            ndb->closeTransaction(trans);
            response->clear();
            failed_get_operation(response);
            return RONDB_INTERNAL_ERROR;
        }

        NdbTransaction::ExecType commit_type = is_last_row ? NdbTransaction::Commit : NdbTransaction::NoCommit;
        if (trans->execute(commit_type,
                           NdbOperation::AbortOnError) != 0)
        {
            response->clear();
            failed_read_error(response, trans->getNdbError().code);
            ndb->closeTransaction(trans);
            return RONDB_INTERNAL_ERROR;
        }

        for (Uint32 i = 0; i < read_index; i++)
        {
            // Transfer char pointer to response's string
            Uint32 row_value_len =
                value_rows[i].value[0] + (value_rows[i].value[1] << 8);
            response->append(&value_rows[i].value[2], row_value_len);
        }
    }
    return 0;
}

int get_complex_key_row(std::string *response,
                        const NdbDictionary::Dictionary *dict,
                        const NdbDictionary::Table *tab,
                        Ndb *ndb,
                        struct key_table *key_row,
                        Uint32 key_len)
{
    /**
     * Since a simple read using CommittedRead we will go back to
     * the safe method where we first read with lock the key row
     * followed by reading the value rows.
     */
    NdbTransaction *trans = ndb->startTransaction(tab,
                                                  &key_row->key_val[0],
                                                  key_len + 2);
    if (trans == nullptr)
    {
        failed_create_transaction(response, ndb->getNdbError().code);
        return RONDB_INTERNAL_ERROR;
    }
    /**
     * Mask and options means simply reading all columns
     * except primary key column.
     */

    const Uint32 mask = 0xFE;
    const unsigned char *mask_ptr = (const unsigned char *)&mask;
    const NdbOperation *read_op = trans->readTuple(
        pk_key_record,
        (const char *)key_row,
        entire_key_record,
        (char *)key_row,
        NdbOperation::LM_Read, // Shared lock so that reads from value table later are consistent
        mask_ptr);
    if (read_op == nullptr)
    {
        ndb->closeTransaction(trans);
        failed_get_operation(response);
        return RONDB_INTERNAL_ERROR;
    }
    if (trans->execute(NdbTransaction::NoCommit,
                       NdbOperation::AbortOnError) != 0)
    {
        failed_read_error(response,
                          trans->getNdbError().code);
        return RONDB_INTERNAL_ERROR;
    }

    // Got inline value, now getting the other value rows

    // Preparing response based on returned total value length
    char buf[20];
    int len = write_formatted(buf,
                              sizeof(buf),
                              "$%u\r\n",
                              key_row->tot_value_len);
    response->reserve(key_row->tot_value_len + len + 3);
    response->append(buf);

    // Append inline value to response
    Uint32 inline_value_len = key_row->value[0] + (key_row->value[1] << 8);
    response->append((const char *)&key_row->value[2], inline_value_len);

    int ret_code = get_value_rows(response,
                                  ndb,
                                  dict,
                                  trans,
                                  key_row->num_rows,
                                  key_row->key_id,
                                  key_row->tot_value_len);
    ndb->closeTransaction(trans);
    if (ret_code == 0)
    {
        response->append("\r\n");
        return 0;
    }
    return RONDB_INTERNAL_ERROR;
}

int rondb_get_key_id(const NdbDictionary::Table *tab,
                     Uint64 &key_id,
                     Ndb *ndb,
                     std::string *response)
{
    if (ndb->getAutoIncrementValue(tab, key_id, unsigned(1024)) != 0)
    {
        if (ndb->getNdbError().code == 626)
        {
            if (ndb->setAutoIncrementValue(tab, Uint64(1), false) != 0)
            {
                append_response(response,
                                "RonDB Error: Failed to create autoincrement value: ",
                                ndb->getNdbError().code);
                return -1;
            }
            key_id = Uint64(1);
        }
        else
        {
            append_response(response,
                            "RonDB Error: Failed to get autoincrement value: ",
                            ndb->getNdbError().code);
            return -1;
        }
    }
    return 0;
}
