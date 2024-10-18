#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "../common.h"
#include "init.h"

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

int create_key_value_row(std::string *response,
                         Ndb *ndb,
                         const NdbDictionary::Dictionary *dict,
                         NdbTransaction *trans,
                         const char *start_value_ptr,
                         Uint64 key_id,
                         Uint32 this_value_len,
                         Uint32 ordinal,
                         char *buf)
{
    const NdbDictionary::Table *tab = dict->getTable("redis_key_values");
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
                       struct key_table *row,
                       Uint32 key_len)
{
    // This is (usually) a local operation to calculate the correct data node, using the
    // hash of the pk value.
    NdbTransaction *trans = ndb->startTransaction(tab,
                                                  &row->key_val[0],
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
        (const char *)row,
        entire_key_record,
        (char *)row,
        NdbOperation::LM_CommittedRead,
        mask_ptr);
    if (read_op == nullptr)
    {
        ndb->closeTransaction(trans);
        failed_get_operation(response);
        return RONDB_INTERNAL_ERROR;
    }
    if (trans->execute(NdbTransaction::Commit,
                       NdbOperation::AbortOnError) != -1 &&
        read_op->getNdbError().code == 0)
    {
        if (row->num_rows > 0)
        {
            return READ_VALUE_ROWS;
        }
        char buf[20];
        int len = write_formatted(buf,
                                  sizeof(buf),
                                  "$%u\r\n",
                                  row->tot_value_len);
        response->reserve(row->tot_value_len + len + 3);
        response->append(buf);
        response->append((const char *)&row->value[2], row->tot_value_len);
        response->append("\r\n");
        printf("Respond with len: %d, %u tot_value_len, string: %s, string_len: %u\n", len, row->tot_value_len, response->c_str(), Uint32(response->length()));
        ndb->closeTransaction(trans);
        return 0;
    }
    int ret_code = read_op->getNdbError().code;
    if (ret_code == READ_ERROR)
    {
        failed_no_such_row_error(response);
        return READ_ERROR;
    }
    failed_read_error(response, ret_code);
    return RONDB_INTERNAL_ERROR;
}

int get_value_rows(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Dictionary *dict,
                   NdbTransaction *trans,
                   const Uint32 num_rows,
                   const Uint64 key_id,
                   const Uint32 this_value_len,
                   const Uint32 tot_value_len)
{
    const NdbDictionary::Table *tab = dict->getTable("redis_key_values");
    if (tab == nullptr)
    {
        failed_create_table(response, ndb->getNdbError().code);
        ndb->closeTransaction(trans);
        response->clear();
        return -1;
    }
    struct value_table row[2];
    row[0].key_id = key_id;
    row[1].key_id = key_id;
    Uint32 row_index = 0;
    for (Uint32 index = 0; index < num_rows; index++)
    {
        row[row_index].ordinal = index;
        const NdbOperation *read_op = trans->readTuple(
            pk_value_record,
            (const char *)&row,
            entire_value_record,
            (char *)&row,
            NdbOperation::LM_CommittedRead);
        if (read_op == nullptr)
        {
            ndb->closeTransaction(trans);
            response->clear();
            failed_get_operation(response);
            return RONDB_INTERNAL_ERROR;
        }
        row_index++;
        if (row_index == 2 || index == (num_rows - 1))
        {
            row_index = 0;
            NdbTransaction::ExecType commit_type = NdbTransaction::NoCommit;
            if (index == (num_rows - 1))
            {
                commit_type = NdbTransaction::Commit;
            }
            if (trans->execute(commit_type,
                               NdbOperation::AbortOnError) != -1)
            {
                for (Uint32 i = 0; i < row_index; i++)
                {
                    Uint32 this_value_len =
                        row[i].value[0] + (row[i].value[1] << 8);
                    response->append(&row[i].value[2], this_value_len);
                }
            }
            else
            {
                response->clear();
                failed_read_error(response, trans->getNdbError().code);
                return RONDB_INTERNAL_ERROR;
            }
        }
    }
    return 0;
}

int get_complex_key_row(std::string *response,
                        const NdbDictionary::Dictionary *dict,
                        const NdbDictionary::Table *tab,
                        Ndb *ndb,
                        struct key_table *row,
                        Uint32 key_len)
{
    /**
     * Since a simple read using CommittedRead we will go back to
     * the safe method where we first read with lock the key row
     * followed by reading the value rows.
     */
    NdbTransaction *trans = ndb->startTransaction(tab,
                                                  &row->key_val[0],
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
        (const char *)row,
        entire_key_record,
        (char *)row,
        NdbOperation::LM_Read,
        mask_ptr);
    if (read_op == nullptr)
    {
        ndb->closeTransaction(trans);
        failed_get_operation(response);
        return RONDB_INTERNAL_ERROR;
    }
    if (trans->execute(NdbTransaction::NoCommit,
                       NdbOperation::AbortOnError) != -1)
    {
        char buf[20];
        int len = write_formatted(buf,
                                  sizeof(buf),
                                  "$%u\r\n",
                                  row->tot_value_len);

        response->reserve(row->tot_value_len + len + 3);
        response->append(buf);
        Uint32 this_value_len = row->value[0] + (row->value[1] << 8);
        response->append((const char *)&row->value[2], this_value_len);
        int ret_code = get_value_rows(response,
                                      ndb,
                                      dict,
                                      trans,
                                      row->num_rows,
                                      row->key_id,
                                      this_value_len,
                                      row->tot_value_len);
        if (ret_code == 0)
        {
            response->append("\r\n");
            return 0;
        }
        return RONDB_INTERNAL_ERROR;
    }
    failed_read_error(response,
                      trans->getNdbError().code);
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
