#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "../common.h"
#include "db_operations.h"
#include "table_definitions.h"
#include "interpreted_code.h"

NdbRecord *pk_key_record = nullptr;
NdbRecord *entire_key_record = nullptr;
NdbRecord *pk_value_record = nullptr;
NdbRecord *entire_value_record = nullptr;

void set_length(char *buf, Uint32 key_len)
{
    Uint8 *ptr = (Uint8 *)buf;
    ptr[0] = (Uint8)(key_len & 255);
    ptr[1] = (Uint8)(key_len >> 8);
}

Uint32 get_length(char *buf)
{
    Uint8 *ptr = (Uint8 *)buf;
    Uint8 low = ptr[0];
    Uint8 high = ptr[1];
    Uint32 len32 = Uint32(low) + Uint32(256) * Uint32(high);
    return len32;
}

int create_key_row(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Table *tab,
                   NdbTransaction *trans,
                   Uint64 rondb_key,
                   const char *key_str,
                   Uint32 key_len,
                   const char *value_str,
                   Uint32 tot_value_len,
                   Uint32 num_value_rows,
                   Uint32 row_state,
                   char *buf)
{
    NdbOperation *write_op = trans->getNdbOperation(tab);
    if (write_op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   trans->getNdbError());
        return -1;
    }
    write_op->writeTuple();
    write_data_to_key_op(write_op,
                         rondb_key,
                         key_str,
                         key_len,
                         value_str,
                         tot_value_len,
                         num_value_rows,
                         row_state,
                         buf);
    {
        if (write_op->getNdbError().code != 0)
        {
            assign_ndb_err_to_response(response,
                                       FAILED_DEFINE_OP,
                                       write_op->getNdbError());
            return -1;
        }
    }
    {
        int ret_code = 0;
        if (num_value_rows == 0)
        {
            if (trans->execute(NdbTransaction::Commit,
                               NdbOperation::AbortOnError) == 0 &&
                trans->getNdbError().code == 0)
            {
                return 0;
            }
        }
        else
        {
            if (trans->execute(NdbTransaction::NoCommit,
                               NdbOperation::AbortOnError) == 0 &&
                trans->getNdbError().code == 0)
            {
                return 0;
            }
        }

        if (trans->getNdbError().code != FOREIGN_KEY_RESTRICT_ERROR)
        {
            assign_ndb_err_to_response(response,
                                       FAILED_EXEC_TXN,
                                       trans->getNdbError());
        }
        return trans->getNdbError().code;
    }
}

int delete_and_insert_key_row(std::string *response,
                              Ndb *ndb,
                              const NdbDictionary::Table *tab,
                              NdbTransaction *trans,
                              Uint64 rondb_key,
                              const char *key_str,
                              Uint32 key_len,
                              const char *value_str,
                              Uint32 tot_value_len,
                              Uint32 num_value_rows,
                              Uint32 row_state,
                              char *buf)
{
    if (delete_key_row(response,
                       ndb,
                       tab,
                       trans,
                       key_str,
                       key_len,
                       buf) != 0)
    {
        return -1;
    }

    return insert_key_row(response,
                          ndb,
                          tab,
                          trans,
                          rondb_key,
                          key_str,
                          key_len,
                          value_str,
                          tot_value_len,
                          num_value_rows,
                          row_state,
                          buf);
}

int delete_key_row(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Table *tab,
                   NdbTransaction *trans,
                   const char *key_str,
                   Uint32 key_len,
                   char *buf)
{
    NdbOperation *del_op = trans->getNdbOperation(tab);
    if (del_op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   trans->getNdbError());
        return -1;
    }
    del_op->deleteTuple();
    memcpy(&buf[2], key_str, key_len);
    set_length(buf, key_len);
    del_op->equal(KEY_TABLE_COL_redis_key, buf);

    if (del_op->getNdbError().code != 0)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_DEFINE_OP,
                                   del_op->getNdbError());
        return -1;
    }

    if (trans->execute(NdbTransaction::NoCommit,
                       NdbOperation::AbortOnError) != 0 ||
        trans->getNdbError().code != 0)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_EXEC_TXN,
                                   trans->getNdbError());
        return -1;
    }
    return 0;
}

int insert_key_row(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Table *tab,
                   NdbTransaction *trans,
                   Uint64 rondb_key,
                   const char *key_str,
                   Uint32 key_len,
                   const char *value_str,
                   Uint32 tot_value_len,
                   Uint32 num_value_rows,
                   Uint32 row_state,
                   char *buf)
{
    {
        NdbOperation *insert_op = trans->getNdbOperation(tab);
        if (insert_op == nullptr)
        {
            assign_ndb_err_to_response(response,
                                       FAILED_GET_OP,
                                       trans->getNdbError());
            return -1;
        }
        insert_op->insertTuple();
        write_data_to_key_op(insert_op,
                             rondb_key,
                             key_str,
                             key_len,
                             value_str,
                             tot_value_len,
                             num_value_rows,
                             row_state,
                             buf);
        if (insert_op->getNdbError().code != 0)
        {
            assign_ndb_err_to_response(response,
                                       FAILED_DEFINE_OP,
                                       insert_op->getNdbError());
            return -1;
        }
    }
    {
        if (num_value_rows == 0)
        {
            if (trans->execute(NdbTransaction::Commit,
                               NdbOperation::AbortOnError) == 0 &&
                trans->getNdbError().code == 0)
            {
                return 0;
            }
        }
        else
        {
            if (trans->execute(NdbTransaction::NoCommit,
                               NdbOperation::AbortOnError) == 0 &&
                trans->getNdbError().code == 0)
            {
                return 0;
            }
        }
        assign_ndb_err_to_response(response,
                                   FAILED_EXEC_TXN,
                                   trans->getNdbError());
        return -1;
    }
}

void write_data_to_key_op(NdbOperation *ndb_op,
                          Uint64 rondb_key,
                          const char *key_str,
                          Uint32 key_len,
                          const char *value_str,
                          Uint32 tot_value_len,
                          Uint32 num_value_rows,
                          Uint32 row_state,
                          char *buf)
{
    memcpy(&buf[2], key_str, key_len);
    set_length(buf, key_len);
    ndb_op->equal(KEY_TABLE_COL_redis_key, buf);

    if (rondb_key == 0)
    {
        ndb_op->setValue(KEY_TABLE_COL_rondb_key, (char *)NULL);
    }
    else
    {
        ndb_op->setValue(KEY_TABLE_COL_rondb_key, rondb_key);
    }
    ndb_op->setValue(KEY_TABLE_COL_tot_value_len, tot_value_len);
    ndb_op->setValue(KEY_TABLE_COL_num_rows, num_value_rows);
    ndb_op->setValue(KEY_TABLE_COL_value_data_type, row_state);
    ndb_op->setValue(KEY_TABLE_COL_expiry_date, 0);

    Uint32 this_value_len = tot_value_len;
    if (this_value_len > INLINE_VALUE_LEN)
    {
        this_value_len = INLINE_VALUE_LEN;
    }
    memcpy(&buf[2], value_str, this_value_len);
    set_length(buf, this_value_len);
    ndb_op->setValue(KEY_TABLE_COL_value_start, buf);
}

int create_value_row(std::string *response,
                     Ndb *ndb,
                     const NdbDictionary::Dictionary *dict,
                     NdbTransaction *trans,
                     const char *start_value_ptr,
                     Uint64 rondb_key,
                     Uint32 this_value_len,
                     Uint32 ordinal,
                     char *buf)
{
    const NdbDictionary::Table *tab = dict->getTable(VALUE_TABLE_NAME);
    if (tab == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TABLE_OBJECT,
                                   ndb->getNdbError());
        return -1;
    }
    NdbOperation *op = trans->getNdbOperation(tab);
    if (op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   trans->getNdbError());
        return -1;
    }
    op->insertTuple();
    op->equal(VALUE_TABLE_COL_rondb_key, rondb_key);
    op->equal(VALUE_TABLE_COL_ordinal, ordinal);
    memcpy(&buf[2], start_value_ptr, this_value_len);
    set_length(buf, this_value_len);
    op->setValue(VALUE_TABLE_COL_value, buf);
    {
        if (op->getNdbError().code != 0)
        {
            assign_ndb_err_to_response(response, FAILED_DEFINE_OP, op->getNdbError());
            return -1;
        }
    }
    return 0;
}

int create_all_value_rows(std::string *response,
                          Ndb *ndb,
                          const NdbDictionary::Dictionary *dict,
                          NdbTransaction *trans,
                          Uint64 rondb_key,
                          const char *value_str,
                          Uint32 value_len,
                          Uint32 num_value_rows,
                          char *buf)
{
    Uint32 remaining_len = value_len - INLINE_VALUE_LEN;
    const char *start_value_ptr = &value_str[INLINE_VALUE_LEN];
    for (Uint32 ordinal = 0; ordinal < num_value_rows; ordinal++)
    {
        Uint32 this_value_len = remaining_len;
        if (remaining_len > EXTENSION_VALUE_LEN)
        {
            this_value_len = EXTENSION_VALUE_LEN;
        }
        if (create_value_row(response,
                             ndb,
                             dict,
                             trans,
                             start_value_ptr,
                             rondb_key,
                             this_value_len,
                             ordinal,
                             buf) != 0)
        {
            return -1;
        }
        remaining_len -= this_value_len;
        start_value_ptr += this_value_len;
    }

    if (trans->execute(NdbTransaction::Commit,
                       NdbOperation::AbortOnError) != 0 ||
        trans->getNdbError().code != 0)
    {
        assign_ndb_err_to_response(response, FAILED_EXEC_TXN, trans->getNdbError());
        return -1;
    }

    response->append("+OK\r\n");
    return 0;
}

int get_simple_key_row(std::string *response,
                       const NdbDictionary::Table *tab,
                       Ndb *ndb,
                       NdbTransaction *trans,
                       struct key_table *key_row,
                       Uint32 key_len)
{
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
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   trans->getNdbError());
        return RONDB_INTERNAL_ERROR;
    }
    if (trans->execute(NdbTransaction::Commit,
                       NdbOperation::AbortOnError) != 0 ||
        read_op->getNdbError().code != 0)
    {
        if (read_op->getNdbError().classification == NdbError::NoDataFound)
        {
            response->assign(REDIS_NO_SUCH_KEY);
            return READ_ERROR;
        }
        assign_ndb_err_to_response(response,
                                   FAILED_READ_KEY,
                                   read_op->getNdbError());
        return RONDB_INTERNAL_ERROR;
    }

    if (key_row->num_rows > 0)
    {
        return 0;
    }
    char header_buf[20];
    int header_len = snprintf(header_buf,
                              sizeof(header_buf),
                              "$%u\r\n",
                              key_row->tot_value_len);

    // The total length of the expected response
    response->reserve(header_len + key_row->tot_value_len + 2);
    response->append(header_buf);
    response->append((const char *)&key_row->value_start[2], key_row->tot_value_len);
    response->append("\r\n");
    /*
        printf("Respond with tot_value_len: %u, string: %s\n",
           key_row->tot_value_len,
           (const char *)&key_row->value_start[2], key_row->tot_value_len);
    */
    return 0;
}

int get_value_rows(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Dictionary *dict,
                   NdbTransaction *trans,
                   const Uint32 num_rows,
                   const Uint64 rondb_key,
                   const Uint32 tot_value_len)
{
    const NdbDictionary::Table *tab = dict->getTable(VALUE_TABLE_NAME);
    if (tab == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TABLE_OBJECT,
                                   ndb->getNdbError());
        return -1;
    }

    // This is rounded up
    Uint32 num_read_batches = (num_rows + ROWS_PER_READ - 1) / ROWS_PER_READ;
    for (Uint32 batch = 0; batch < num_read_batches; batch++)
    {
        Uint32 start_ordinal = batch * ROWS_PER_READ;
        Uint32 num_rows_to_read = std::min(ROWS_PER_READ, num_rows - start_ordinal);

        bool is_last_batch = (batch == (num_read_batches - 1));
        NdbTransaction::ExecType commit_type = is_last_batch ? NdbTransaction::Commit : NdbTransaction::NoCommit;

        if (read_batched_value_rows(response,
                                    trans,
                                    rondb_key,
                                    num_rows_to_read,
                                    start_ordinal,
                                    commit_type) != 0)
        {
            return -1;
        }
    }
    return 0;
}

// Break up fetching large values to avoid blocking the network for other reads
int read_batched_value_rows(std::string *response,
                            NdbTransaction *trans,
                            const Uint64 rondb_key,
                            const Uint32 num_rows_to_read,
                            const Uint32 start_ordinal,
                            const NdbTransaction::ExecType commit_type)
{
    struct value_table value_rows[ROWS_PER_READ];

    Uint32 ordinal = start_ordinal;
    for (Uint32 i = 0; i < num_rows_to_read; i++)
    {
        value_rows[i].rondb_key = rondb_key;
        value_rows[i].ordinal = ordinal;
        const NdbOperation *read_op = trans->readTuple(
            pk_value_record,
            (const char *)&value_rows[i],
            entire_value_record,
            (char *)&value_rows[i],
            NdbOperation::LM_CommittedRead);
        if (read_op == nullptr)
        {
            assign_ndb_err_to_response(response,
                                       FAILED_GET_OP,
                                       trans->getNdbError());
            return -1;
        }
        ordinal++;
    }

    if (trans->execute(commit_type,
                       NdbOperation::AbortOnError) != 0 ||
        trans->getNdbError().code != 0)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_READ_KEY,
                                   trans->getNdbError());
        return -1;
    }

    for (Uint32 i = 0; i < num_rows_to_read; i++)
    {
        // Transfer char pointer to response's string
        Uint32 row_value_len = get_length((char *)&value_rows->value[0]);
        response->append((const char *)&value_rows[i].value[2], row_value_len);
    }
    return 0;
}

int get_complex_key_row(std::string *response,
                        const NdbDictionary::Dictionary *dict,
                        const NdbDictionary::Table *tab,
                        Ndb *ndb,
                        NdbTransaction *trans,
                        struct key_table *key_row,
                        Uint32 key_len)
{
    /**
     * Since a simple read using CommittedRead we will go back to
     * the safe method where we first read with lock the key row
     * followed by reading the value rows.
     */
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
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   trans->getNdbError());
        return RONDB_INTERNAL_ERROR;
    }
    if (trans->execute(NdbTransaction::NoCommit,
                       NdbOperation::AbortOnError) != 0 ||
        trans->getNdbError().code != 0)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_READ_KEY,
                                   trans->getNdbError());
        return RONDB_INTERNAL_ERROR;
    }

    // Got inline value, now getting the other value rows

    // Writing the Redis header to the response (indicating value length)
    char header_buf[20];
    int header_len = snprintf(header_buf,
                              sizeof(header_buf),
                              "$%u\r\n",
                              key_row->tot_value_len);
    response->reserve(header_len + key_row->tot_value_len + 2);
    response->append(header_buf);

    // Append inline value to response
    Uint32 inline_value_len = get_length((char *)&key_row->value_start[0]);
    response->append((const char *)&key_row->value_start[2], inline_value_len);

    int ret_code = get_value_rows(response,
                                  ndb,
                                  dict,
                                  trans,
                                  key_row->num_rows,
                                  key_row->rondb_key,
                                  key_row->tot_value_len);
    if (ret_code == 0)
    {
        response->append("\r\n");
        return 0;
    }
    return RONDB_INTERNAL_ERROR;
}

int rondb_get_rondb_key(const NdbDictionary::Table *tab,
                        Uint64 &rondb_key,
                        Ndb *ndb,
                        std::string *response)
{
    if (ndb->getAutoIncrementValue(tab, rondb_key, unsigned(1024)) != 0)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to get autoincrement value",
                                   ndb->getNdbError());
        return -1;
    }
    return 0;
}

void incr_key_row(std::string *response,
                  Ndb *ndb,
                  const NdbDictionary::Table *tab,
                  NdbTransaction *trans,
                  struct key_table *key_row)
{
    /**
     * The mask specifies which columns is to be updated after the interpreter
     * has finished. The values are set in the key_row.
     * We have 7 columns, we will update tot_value_len in interpreter, same with
     * value_start.
     *
     * The rest, redis_key, rondb_key, value_data_type, num_rows and expiry_date
     * are updated through final update.
     */

    const Uint32 mask = 0x55;
    const unsigned char *mask_ptr = (const unsigned char *)&mask;

    // redis_key already set as this is the Primary key
    key_row->null_bits = 1; // Set rondb_key to NULL, first NULL column
    key_row->num_rows = 0;
    key_row->value_data_type = 0;
    key_row->expiry_date = 0;

    Uint32 code_buffer[128];
    NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
    if (initNdbCodeIncr(response, &code, tab) != 0)
        return;

    // Prepare the interpreted program to be part of the write
    NdbOperation::OperationOptions opts;
    std::memset(&opts, 0, sizeof(opts));
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
    opts.interpretedCode = &code;

    /**
     * Prepare to get the final value of the Redis row after INCR is finished
     * This is performed by the reading the pseudo column that is reading the
     * output index written in interpreter program.
     */
    NdbOperation::GetValueSpec getvals[1];
    getvals[0].appStorage = nullptr;
    getvals[0].recAttr = nullptr;
    getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
    opts.numExtraGetFinalValues = 1;
    opts.extraGetFinalValues = getvals;

    if (1)
        opts.optionsPresent |= NdbOperation::OperationOptions::OO_DIRTY_FLAG;

    /* Define the actual operation to be sent to RonDB data node. */
    const NdbOperation *op = trans->writeTuple(
        pk_key_record,
        (const char *)key_row,
        entire_key_record,
        (char *)key_row,
        mask_ptr,
        &opts,
        sizeof(opts));
    if (op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to create NdbOperation",
                                   trans->getNdbError());
        return;
    }

    /* Send to RonDB and execute the INCR operation */
    if (trans->execute(NdbTransaction::Commit,
                       NdbOperation::AbortOnError) != 0 ||
        trans->getNdbError().code != 0)
    {
        if (trans->getNdbError().code == RONDB_KEY_NOT_NULL_ERROR)
        {
            assign_ndb_err_to_response(response,
                                       FAILED_INCR_KEY_MULTI_ROW,
                                       trans->getNdbError());
            return;
        }
        assign_ndb_err_to_response(response,
                                   FAILED_INCR_KEY,
                                   trans->getNdbError());
        return;
    }

    /* Retrieve the returned new value as an Int64 value */
    NdbRecAttr *recAttr = getvals[0].recAttr;
    Int64 new_incremented_value = recAttr->int64_value();

    /* Send the return message to Redis client */
    char header_buf[20];
    int header_len = snprintf(header_buf,
                              sizeof(header_buf),
                              ":%lld\r\n",
                              new_incremented_value);
    response->assign(header_buf);
    return;
}
