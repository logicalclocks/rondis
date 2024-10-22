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
    const NdbDictionary::Table *tab = dict->getTable(KEY_TABLE_NAME);
    if (tab == nullptr)
    {
        printf("Failed getting Ndb table %s\n", KEY_TABLE_NAME);
        return -1;
    }

    const NdbDictionary::Column *redis_key_col = tab->getColumn(KEY_TABLE_COL_redis_key);
    const NdbDictionary::Column *rondb_key_col = tab->getColumn(KEY_TABLE_COL_rondb_key);
    const NdbDictionary::Column *expiry_date_col = tab->getColumn(KEY_TABLE_COL_expiry_date);
    const NdbDictionary::Column *value_start_col = tab->getColumn(KEY_TABLE_COL_value_start);
    const NdbDictionary::Column *tot_value_len_col = tab->getColumn(KEY_TABLE_COL_tot_value_len);
    const NdbDictionary::Column *num_rows_col = tab->getColumn(KEY_TABLE_COL_num_rows);
    const NdbDictionary::Column *value_data_type_col = tab->getColumn(KEY_TABLE_COL_value_data_type);

    if (redis_key_col == nullptr ||
        rondb_key_col == nullptr ||
        expiry_date_col == nullptr ||
        value_start_col == nullptr ||
        tot_value_len_col == nullptr ||
        num_rows_col == nullptr ||
        value_data_type_col == nullptr)
    {
        printf("Failed getting Ndb columns for table %s\n", KEY_TABLE_NAME);
        return -1;
    }

    printf("Creating records for table %s\n", KEY_TABLE_NAME);
    const int NUM_COLS_PK_LOOKUP = 1;
    NdbDictionary::RecordSpecification pk_lookup_specs[NUM_COLS_PK_LOOKUP];

    pk_lookup_specs[0].column = redis_key_col;
    pk_lookup_specs[0].offset = offsetof(struct key_table, redis_key);
    pk_lookup_specs[0].nullbit_byte_offset = 0;
    pk_lookup_specs[0].nullbit_bit_in_byte = 0;
    pk_key_record = dict->createRecord(tab,
                                       pk_lookup_specs,
                                       NUM_COLS_PK_LOOKUP,
                                       sizeof(pk_lookup_specs[0]));
    if (pk_key_record == nullptr)
    {
        printf("Failed creating pk-lookup record for table %s\n", KEY_TABLE_NAME);
        return -1;
    }

    std::map<const NdbDictionary::Column *, std::pair<size_t, int>> column_info_map = {
        {redis_key_col, {offsetof(struct key_table, redis_key), 0}},
        {rondb_key_col, {offsetof(struct key_table, rondb_key), 0}},
        // TODO: This column is supposed to be nullable
        {expiry_date_col, {offsetof(struct key_table, expiry_date), 0}},
        {value_start_col, {offsetof(struct key_table, value_start), 0}},
        {tot_value_len_col, {offsetof(struct key_table, tot_value_len), 0}},
        {num_rows_col, {offsetof(struct key_table, num_rows), 0}},
        {value_data_type_col, {offsetof(struct key_table, value_data_type), 0}}
    };

    NdbDictionary::RecordSpecification read_all_cols_specs[column_info_map.size()];
    int i = 0;
    for (const auto &entry : column_info_map)
    {
        read_all_cols_specs[i].column = entry.first;
        read_all_cols_specs[i].offset = entry.second.first;
        read_all_cols_specs[i].nullbit_byte_offset = 0;
        read_all_cols_specs[i].nullbit_bit_in_byte = entry.second.second;
        ++i;
    }

    entire_key_record = dict->createRecord(tab,
                                           read_all_cols_specs,
                                           column_info_map.size(),
                                           sizeof(read_all_cols_specs[0]));
    if (entire_key_record == nullptr)
    {
        printf("Failed creating read-all cols record for table %s\n", KEY_TABLE_NAME);
        return -1;
    }
    return 0;
}

int init_value_records(NdbDictionary::Dictionary *dict)
{
    printf("Getting table %s\n", VALUE_TABLE_NAME);
    const NdbDictionary::Table *tab = dict->getTable(VALUE_TABLE_NAME);
    if (tab == nullptr)
    {
        printf("Failed getting Ndb table %s\n", VALUE_TABLE_NAME);
        return -1;
    }

    const NdbDictionary::Column *rondb_key_col = tab->getColumn(VALUE_TABLE_COL_rondb_key);
    const NdbDictionary::Column *ordinal_col = tab->getColumn(VALUE_TABLE_COL_ordinal);
    const NdbDictionary::Column *value_col = tab->getColumn(VALUE_TABLE_COL_value);
    if (rondb_key_col == nullptr ||
        ordinal_col == nullptr ||
        value_col == nullptr)
    {
        printf("Failed getting Ndb columns for table %s\n", VALUE_TABLE_NAME);
        return -1;
    }

    const int NUM_COLS_PK_LOOKUP = 2;
    NdbDictionary::RecordSpecification pk_lookup_specs[NUM_COLS_PK_LOOKUP];

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
                                         NUM_COLS_PK_LOOKUP,
                                         sizeof(pk_lookup_specs[0]));
    if (pk_value_record == nullptr)
    {
        printf("Failed creating pk-lookup record for table %s\n", VALUE_TABLE_NAME);
        return -1;
    }

    const int NUM_COLS_READ_ALL_COLS = 3;
    NdbDictionary::RecordSpecification read_all_cols_specs[NUM_COLS_READ_ALL_COLS];

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
                                             NUM_COLS_READ_ALL_COLS,
                                             sizeof(read_all_cols_specs[0]));
    if (entire_value_record == nullptr)
    {
        printf("Failed creating read-all cols record for table %s\n", VALUE_TABLE_NAME);
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
