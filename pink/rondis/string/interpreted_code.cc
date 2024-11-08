#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "../common.h"
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
    code->convert_size(REG3, REG0);                 // Write back length bytes in memory

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
