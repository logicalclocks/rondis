#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "../common.h"
#include "commands.h"
#include "interpreted_code.h"
#include "table_definitions.h"

// Define the interpreted program for the INCR operation
int initNdbCodeIncr(std::string *response,
                    NdbInterpretedCode *code,
                    const NdbDictionary::Table *tab)
{
    const NdbDictionary::Column *value_start_col = tab->getColumn(KEY_TABLE_COL_value_start);
    const NdbDictionary::Column *tot_value_len_col = tab->getColumn(KEY_TABLE_COL_tot_value_len);
    const NdbDictionary::Column *rondb_key_col = tab->getColumn(KEY_TABLE_COL_rondb_key);

    code->load_const_u16(REG0, MEMORY_OFFSET_LEN_BYTES);
    code->load_const_u16(REG6, MEMORY_OFFSET_START);
    code->load_op_type(REG1);                          // Read operation type into register 1
    code->branch_eq_const(REG1, RONDB_INSERT, LABEL1); // Inserts go to label 1

    /**
     * The first 4 bytes of the memory must be kept for the Attribute header
     * REG0 Memory offset == 4
     * REG1 Memory offset == 6
     * REG2 Size of value_start
     * REG3 Size of value_start without length bytes
     * REG4 Old integer value after conversion
     * REG5 New integer value after increment
     * REG6 Memory offset == 0
     * REG7 Value of rondb_key (should be NULL)
     */
    /* UPDATE code */
    code->read_attr(REG7, rondb_key_col);
    code->branch_eq_null(REG7, LABEL0);
    code->interpret_exit_nok(RONDB_KEY_NOT_NULL_ERROR);
    code->def_label(LABEL0);
    code->read_full(value_start_col, REG6, REG2); // Read value_start column
    code->load_const_u16(REG1, MEMORY_OFFSET_STRING);
    code->sub_const_reg(REG3, REG2, NUM_LEN_BYTES);
    code->str_to_int64(REG4, REG1, REG3); // Convert string to number
    code->add_const_reg(REG5, REG4, INCREMENT_VALUE);
    code->int64_to_str(REG3, REG1, REG5);           // Convert number to string
    code->add_const_reg(REG2, REG3, NUM_LEN_BYTES); // New value_start length
    code->write_size_mem(REG3, REG0);               // Write back length bytes in memory

    code->write_interpreter_output(REG5, OUTPUT_INDEX); // Write into output index 0
    code->write_from_mem(value_start_col, REG6, REG2);  // Write to column
    code->write_attr(tot_value_len_col, REG3);
    code->interpret_exit_ok();

    /* INSERT code */
    code->def_label(LABEL1);
    code->load_const_u16(REG5, INITIAL_INT_VALUE);
    code->load_const_u16(REG3, INITIAL_INT_STRING_LEN);
    code->write_interpreter_output(REG5, OUTPUT_INDEX); // Write into output index 0

    Uint32 insert_value;
    Uint8 *insert_value_ptr = (Uint8 *)&insert_value;
    insert_value_ptr[0] = 1;                  // Length is 1
    insert_value_ptr[1] = 0;                  // Second length byte is 0
    insert_value_ptr[2] = INITIAL_INT_STRING; // Inserts a string '1'
    insert_value_ptr[3] = 0;

    code->load_const_mem(REG0,
                         REG2,
                         INITIAL_INT_STRING_LEN_WITH_LEN_BYTES,
                         &insert_value);
    code->write_from_mem(value_start_col, REG6, REG2); // Write to column
    code->write_attr(tot_value_len_col, REG3);
    code->interpret_exit_ok();

    // Program end, now compile code
    int ret_code = code->finalise();
    if (ret_code != 0)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to create Interpreted code",
                                   code->getNdbError());
        return -1;
    }
    return 0;
}

int write_hset_key_table(Ndb *ndb,
                         const NdbDictionary::Table *tab,
                         std::string std_key_str,
                         Uint64 & redis_key_id,
                         std::string *response) {
    /* Prepare primary key */
    struct hset_key_table key_row;
    const char *key_str = std_key_str.c_str();
    Uint32 key_len = std_key_str.size();
    set_length(&key_row.redis_key[0], key_len);
    memcpy(&key_row.redis_key[2], key_str, key_len);

    const Uint32 mask = 0x1; // Write primary key
    const unsigned char *mask_ptr = (const unsigned char *)&mask;
    const NdbDictionary::Column *redis_key_id_col = tab->getColumn(HSET_KEY_TABLE_COL_redis_key_id);
    Uint32 code_buffer[64];
    NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
    code.load_op_type(REG1);                          // Read operation type into register 1
    code.branch_eq_const(REG1, RONDB_INSERT, LABEL0); // Inserts go to label 0
    /* UPDATE */
    code.read_attr(REG7, redis_key_id_col);
    code.write_interpreter_output(REG7, OUTPUT_INDEX); // Write into output index 0
    code.interpret_exit_ok();

    /* INSERT */
    code.def_label(LABEL0);
    code.load_const_u64(REG7, redis_key_id);
    code.write_attr(redis_key_id_col, REG7);
    code.write_interpreter_output(REG7, OUTPUT_INDEX); // Write into output index 0
    code.interpret_exit_ok();

    // Program end, now compile code
    int ret_code = code.finalise();
    if (ret_code != 0)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to create Interpreted code",
                                   code.getNdbError());
        return -1;
    }
    // Prepare the interpreted program to be part of the write
    NdbOperation::OperationOptions opts;
    std::memset(&opts, 0, sizeof(opts));
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
    opts.interpretedCode = &code;

    NdbOperation::GetValueSpec getvals[1];
    getvals[0].appStorage = nullptr;
    getvals[0].recAttr = nullptr;
    getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
    opts.numExtraGetFinalValues = 1;
    opts.extraGetFinalValues = getvals;

    /* Start a transaction */
    NdbTransaction *trans = ndb->startTransaction(tab,
                                                  (const char*)&key_row.redis_key_id,
                                                  key_len + 2);
    /* Define the actual operation to be sent to RonDB data node. */
    const NdbOperation *op = trans->writeTuple(
        pk_hset_key_record,
        (const char *)&key_row,
        entire_hset_key_record,
        (char *)&key_row,
        mask_ptr,
        &opts,
        sizeof(opts));
    if (op == nullptr)
    {
        ndb->closeTransaction(trans);
        assign_ndb_err_to_response(response,
                                   "Failed to create NdbOperation",
                                   trans->getNdbError());
        return -1;
    }
    if (trans->execute(NdbTransaction::Commit,
                       NdbOperation::AbortOnError) != 0 ||
        trans->getNdbError().code != 0)
    {
        ndb->closeTransaction(trans);
        assign_ndb_err_to_response(response,
                                   FAILED_HSET_KEY,
                                   trans->getNdbError());
        return -1;
    }
    /* Retrieve the returned new value as an Uint64 value */
    NdbRecAttr *recAttr = getvals[0].recAttr;
    redis_key_id = recAttr->u_64_value();
    ndb->closeTransaction(trans);
    return 0;
}

int write_key_row_no_commit(std::string *response,
                            NdbInterpretedCode &code,
                            const NdbDictionary::Table *tab) {
    const NdbDictionary::Column *num_rows_col = tab->getColumn(KEY_TABLE_COL_num_rows);
    code.load_op_type(REG1);                          // Read operation type into register 1
    code.branch_eq_const(REG1, RONDB_INSERT, LABEL0); // Inserts go to label 0
    /* UPDATE */
    code.read_attr(REG7, num_rows_col);
    code.write_interpreter_output(REG7, OUTPUT_INDEX); // Write into output index 0
    code.interpret_exit_ok();

    /* INSERT */
    code.def_label(LABEL0);
    code.load_const_u16(REG7, 0);
    code.write_interpreter_output(REG7, OUTPUT_INDEX); // Write into output index 0
    code.interpret_exit_ok();

    // Program end, now compile code
    int ret_code = code.finalise();
    if (ret_code != 0)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to create Interpreted code",
                                   code.getNdbError());
        return -1;
    }
    return 0;
}

int write_key_row_commit(std::string *response,
                         NdbInterpretedCode &code,
                         const NdbDictionary::Table *tab) {
    const NdbDictionary::Column *num_rows_col = tab->getColumn(KEY_TABLE_COL_num_rows);
    code.load_op_type(REG1);                          // Read operation type into register 1
    code.branch_eq_const(REG1, RONDB_INSERT, LABEL0); // Inserts go to label 0
    /* UPDATE */
    code.read_attr(REG7, num_rows_col);
    code.branch_eq_const(REG7, 0, LABEL0);
    code.interpret_exit_nok(6000);

    /* INSERT */
    code.def_label(LABEL0);
    code.interpret_exit_ok();

    // Program end, now compile code
    int ret_code = code.finalise();
    if (ret_code != 0)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to create Interpreted code",
                                   code.getNdbError());
        return -1;
    }
    return 0;
}
