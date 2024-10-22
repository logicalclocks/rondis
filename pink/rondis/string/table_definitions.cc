#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <map>

#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"
#include "../common.h"
#include "table_definitions.h"
#include "commands.h"

/**
 * Create NdbRecord's for all table accesses, they can be reused
 * for all Ndb objects.
 */
int init_key_records(NdbDictionary::Dictionary *dict)
{
    printf("Getting table %s\n", KEY_TABLE_NAME);
    const NdbDictionary::Table *tab = dict->getTable(KEY_TABLE_NAME);
    if (tab == nullptr)
    {
        printf("Failed getting table %s\n", KEY_TABLE_NAME);
        return -1;
    }

    printf("Getting columns for table %s\n", KEY_TABLE_NAME);
    const NdbDictionary::Column *redis_key_col = tab->getColumn(KEY_TABLE_COL_redis_key);
    const NdbDictionary::Column *rondb_key_col = tab->getColumn(KEY_TABLE_COL_rondb_key);
    const NdbDictionary::Column *expiry_date_col = tab->getColumn(KEY_TABLE_COL_expiry_date);
    const NdbDictionary::Column *value_start_col = tab->getColumn(KEY_TABLE_COL_value_start);
    const NdbDictionary::Column *tot_value_len_col = tab->getColumn(KEY_TABLE_COL_tot_value_len);
    const NdbDictionary::Column *num_rows_col = tab->getColumn(KEY_TABLE_COL_num_rows);
    const NdbDictionary::Column *row_state_col = tab->getColumn(KEY_TABLE_COL_value_data_type);

    if (redis_key_col == nullptr ||
        rondb_key_col == nullptr ||
        expiry_date_col == nullptr ||
        value_start_col == nullptr ||
        tot_value_len_col == nullptr ||
        num_rows_col == nullptr ||
        row_state_col == nullptr)
    {
        printf("Failed getting columns for table %s\n", KEY_TABLE_NAME);
        return -1;
    }

    printf("Getting records for table %s\n", KEY_TABLE_NAME);
    NdbDictionary::RecordSpecification pk_lookup_specs[1];
    NdbDictionary::RecordSpecification read_all_cols_specs[7];

    pk_lookup_specs[0].column = redis_key_col;
    pk_lookup_specs[0].offset = offsetof(struct key_table, redis_key);
    pk_lookup_specs[0].nullbit_byte_offset = 0;
    pk_lookup_specs[0].nullbit_bit_in_byte = 0;
    pk_key_record =
        dict->createRecord(tab,
                           pk_lookup_specs,
                           1,
                           sizeof(pk_lookup_specs[0]));
    if (pk_key_record == nullptr)
    {
        printf("Failed creating record for table %s\n", KEY_TABLE_NAME);
        return -1;
    }

    read_all_cols_specs[0].column = redis_key_col;
    read_all_cols_specs[0].offset = offsetof(struct key_table, redis_key);
    read_all_cols_specs[0].nullbit_byte_offset = 0;
    read_all_cols_specs[0].nullbit_bit_in_byte = 0;

    read_all_cols_specs[1].column = rondb_key_col;
    read_all_cols_specs[1].offset = offsetof(struct key_table, rondb_key);
    read_all_cols_specs[1].nullbit_byte_offset = 0;
    read_all_cols_specs[1].nullbit_bit_in_byte = 0;

    read_all_cols_specs[2].column = expiry_date_col;
    read_all_cols_specs[2].offset = offsetof(struct key_table, expiry_date);
    read_all_cols_specs[2].nullbit_byte_offset = 0;
    read_all_cols_specs[2].nullbit_bit_in_byte = 1;

    read_all_cols_specs[3].column = value_start_col;
    read_all_cols_specs[3].offset = offsetof(struct key_table, value_start);
    read_all_cols_specs[3].nullbit_byte_offset = 0;
    read_all_cols_specs[3].nullbit_bit_in_byte = 0;

    read_all_cols_specs[4].column = tot_value_len_col;
    read_all_cols_specs[4].offset = offsetof(struct key_table, tot_value_len);
    read_all_cols_specs[4].nullbit_byte_offset = 0;
    read_all_cols_specs[4].nullbit_bit_in_byte = 0;

    read_all_cols_specs[5].column = num_rows_col;
    read_all_cols_specs[5].offset = offsetof(struct key_table, num_rows);
    read_all_cols_specs[5].nullbit_byte_offset = 0;
    read_all_cols_specs[5].nullbit_bit_in_byte = 0;

    read_all_cols_specs[6].column = row_state_col;
    read_all_cols_specs[6].offset = offsetof(struct key_table, value_data_type);
    read_all_cols_specs[6].nullbit_byte_offset = 0;
    read_all_cols_specs[6].nullbit_bit_in_byte = 0;

    entire_key_record = dict->createRecord(tab,
                                           read_all_cols_specs,
                                           8,
                                           sizeof(read_all_cols_specs[0]));
    if (entire_key_record == nullptr)
    {
        printf("Failed creating record for table %s\n", KEY_TABLE_NAME);
        return -1;
    }
    return 0;
}

int init_value_records(NdbDictionary::Dictionary *dict)
{
    const NdbDictionary::Table *tab = dict->getTable("redis_key_value");
    if (tab == nullptr)
    {
        printf("Failed getting table for table %s\n", VALUE_TABLE_NAME);
        return -1;
    }
    const NdbDictionary::Column *rondb_key_col = tab->getColumn(VALUE_TABLE_COL_rondb_key);
    const NdbDictionary::Column *ordinal_col = tab->getColumn(VALUE_TABLE_COL_ordinal);
    const NdbDictionary::Column *value_col = tab->getColumn(VALUE_TABLE_COL_value);
    if (rondb_key_col == nullptr ||
        ordinal_col == nullptr ||
        value_col == nullptr)
    {
        printf("Failed getting columns for table %s\n", VALUE_TABLE_NAME);
        return -1;
    }

    NdbDictionary::RecordSpecification pk_lookup_specs[2];
    NdbDictionary::RecordSpecification read_all_cols_specs[3];

    pk_lookup_specs[0].column = rondb_key_col;
    pk_lookup_specs[0].offset = offsetof(struct value_table, rondb_key);
    pk_lookup_specs[0].nullbit_byte_offset = 0;
    pk_lookup_specs[0].nullbit_bit_in_byte = 0;

    pk_lookup_specs[1].column = ordinal_col;
    pk_lookup_specs[1].offset = offsetof(struct value_table, ordinal);
    pk_lookup_specs[1].nullbit_byte_offset = 0;
    pk_lookup_specs[1].nullbit_bit_in_byte = 0;

    pk_value_record = dict->createRecord(tab,
                                         pk_lookup_specs,
                                         2,
                                         sizeof(pk_lookup_specs[0]));
    if (pk_value_record == nullptr)
    {
        printf("Failed creating record for table %s\n", VALUE_TABLE_NAME);
        return -1;
    }

    read_all_cols_specs[0].column = rondb_key_col;
    read_all_cols_specs[0].offset = offsetof(struct value_table, rondb_key);
    read_all_cols_specs[0].nullbit_byte_offset = 0;
    read_all_cols_specs[0].nullbit_bit_in_byte = 0;

    read_all_cols_specs[1].column = ordinal_col;
    read_all_cols_specs[1].offset = offsetof(struct value_table, ordinal);
    read_all_cols_specs[1].nullbit_byte_offset = 0;
    read_all_cols_specs[1].nullbit_bit_in_byte = 0;

    read_all_cols_specs[2].column = value_col;
    read_all_cols_specs[2].offset = offsetof(struct value_table, value);
    read_all_cols_specs[2].nullbit_byte_offset = 0;
    read_all_cols_specs[2].nullbit_bit_in_byte = 0;

    entire_value_record = dict->createRecord(tab,
                                             read_all_cols_specs,
                                             3,
                                             sizeof(read_all_cols_specs[0]));
    if (entire_value_record == nullptr)
    {
        printf("Failed creating record for table %s\n", VALUE_TABLE_NAME);
        return -1;
    }

    return 0;
}

int init_string_records(NdbDictionary::Dictionary *dict)
{
    int res = init_key_records(dict);
    if (res != 0)
    {
        return res;
    }

    return init_value_records(dict);
}
