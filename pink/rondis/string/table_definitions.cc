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
        printf("Failed getting table for key table of STRING\n");
        return -1;
    }
    const NdbDictionary::Column *redis_key_col = tab->getColumn(KEY_TABLE_COL_redis_key);
    const NdbDictionary::Column *rondb_key_col = tab->getColumn(KEY_TABLE_COL_rondb_key);
    const NdbDictionary::Column *expiry_date_col = tab->getColumn(KEY_TABLE_COL_expiry_date);
    const NdbDictionary::Column *value_start_col = tab->getColumn(KEY_TABLE_COL_value_start);
    const NdbDictionary::Column *tot_value_len_col = tab->getColumn(KEY_TABLE_COL_tot_value_len);
    const NdbDictionary::Column *num_rows_col = tab->getColumn(KEY_TABLE_COL_num_rows);
    const NdbDictionary::Column *row_state_col = tab->getColumn(KEY_TABLE_COL_row_state);

    if (redis_key_col == nullptr ||
        rondb_key_col == nullptr ||
        expiry_date_col == nullptr ||
        value_start_col == nullptr ||
        tot_value_len_col == nullptr ||
        num_rows_col == nullptr ||
        row_state_col == nullptr)
    {
        printf("Failed getting columns for key table of STRING\n");
        return -1;
    }

    NdbDictionary::RecordSpecification primary_redis_main_key_spec[1];
    NdbDictionary::RecordSpecification all_redis_main_key_spec[7];

    primary_redis_main_key_spec[0].column = redis_key_col;
    primary_redis_main_key_spec[0].offset = offsetof(struct key_table, redis_key);
    primary_redis_main_key_spec[0].nullbit_byte_offset = 0;
    primary_redis_main_key_spec[0].nullbit_bit_in_byte = 0;
    pk_key_record =
        dict->createRecord(tab,
                           primary_redis_main_key_spec,
                           1,
                           sizeof(primary_redis_main_key_spec[0]));
    if (pk_key_record == nullptr)
    {
        printf("Failed creating record for key table of STRING\n");
        return -1;
    }

    all_redis_main_key_spec[0].column = redis_key_col;
    all_redis_main_key_spec[0].offset = offsetof(struct key_table, redis_key);
    all_redis_main_key_spec[0].nullbit_byte_offset = 0;
    all_redis_main_key_spec[0].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[1].column = rondb_key_col;
    all_redis_main_key_spec[1].offset = offsetof(struct key_table, rondb_key);
    all_redis_main_key_spec[1].nullbit_byte_offset = 0;
    all_redis_main_key_spec[1].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[2].column = expiry_date_col;
    all_redis_main_key_spec[2].offset = offsetof(struct key_table, expiry_date);
    all_redis_main_key_spec[2].nullbit_byte_offset = 0;
    all_redis_main_key_spec[2].nullbit_bit_in_byte = 1;

    all_redis_main_key_spec[3].column = value_start_col;
    all_redis_main_key_spec[3].offset = offsetof(struct key_table, value_start);
    all_redis_main_key_spec[3].nullbit_byte_offset = 0;
    all_redis_main_key_spec[3].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[4].column = tot_value_len_col;
    all_redis_main_key_spec[4].offset = offsetof(struct key_table, tot_value_len);
    all_redis_main_key_spec[4].nullbit_byte_offset = 0;
    all_redis_main_key_spec[4].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[5].column = num_rows_col;
    all_redis_main_key_spec[5].offset = offsetof(struct key_table, num_rows);
    all_redis_main_key_spec[5].nullbit_byte_offset = 0;
    all_redis_main_key_spec[5].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[6].column = row_state_col;
    all_redis_main_key_spec[6].offset = offsetof(struct key_table, row_state);
    all_redis_main_key_spec[6].nullbit_byte_offset = 0;
    all_redis_main_key_spec[6].nullbit_bit_in_byte = 0;

    entire_key_record = dict->createRecord(tab,
                                           all_redis_main_key_spec,
                                           8,
                                           sizeof(all_redis_main_key_spec[0]));
    if (entire_key_record == nullptr)
    {
        printf("Failed creating record for key table of STRING\n");
        return -1;
    }
    return 0;
}

int init_value_records(NdbDictionary::Dictionary *dict)
{
    const NdbDictionary::Table *tab = dict->getTable("redis_key_value");
    if (tab == nullptr)
    {
        printf("Failed getting table for value table of STRING\n");
        return -1;
    }
    const NdbDictionary::Column *rondb_key_col = tab->getColumn(VALUE_TABLE_COL_rondb_key);
    const NdbDictionary::Column *ordinal_col = tab->getColumn(VALUE_TABLE_COL_ordinal);
    const NdbDictionary::Column *value_col = tab->getColumn(VALUE_TABLE_COL_value);
    if (rondb_key_col == nullptr ||
        ordinal_col == nullptr ||
        value_col == nullptr)
    {
        printf("Failed getting columns for value table of STRING\n");
        return -1;
    }

    NdbDictionary::RecordSpecification primary_redis_key_value_spec[2];
    NdbDictionary::RecordSpecification all_redis_key_value_spec[3];

    primary_redis_key_value_spec[0].column = rondb_key_col;
    primary_redis_key_value_spec[0].offset = offsetof(struct value_table, rondb_key);
    primary_redis_key_value_spec[0].nullbit_byte_offset = 0;
    primary_redis_key_value_spec[0].nullbit_bit_in_byte = 0;

    primary_redis_key_value_spec[1].column = ordinal_col;
    primary_redis_key_value_spec[1].offset = offsetof(struct value_table, ordinal);
    primary_redis_key_value_spec[1].nullbit_byte_offset = 0;
    primary_redis_key_value_spec[1].nullbit_bit_in_byte = 0;

    pk_value_record = dict->createRecord(tab,
                                         primary_redis_key_value_spec,
                                         2,
                                         sizeof(primary_redis_key_value_spec[0]));
    if (pk_value_record == nullptr)
    {
        printf("Failed creating record for value table of STRING\n");
        return -1;
    }

    all_redis_key_value_spec[0].column = rondb_key_col;
    all_redis_key_value_spec[0].offset = offsetof(struct value_table, rondb_key);
    all_redis_key_value_spec[0].nullbit_byte_offset = 0;
    all_redis_key_value_spec[0].nullbit_bit_in_byte = 0;

    all_redis_key_value_spec[1].column = ordinal_col;
    all_redis_key_value_spec[1].offset = offsetof(struct value_table, ordinal);
    all_redis_key_value_spec[1].nullbit_byte_offset = 0;
    all_redis_key_value_spec[1].nullbit_bit_in_byte = 0;

    all_redis_key_value_spec[2].column = value_col;
    all_redis_key_value_spec[2].offset = offsetof(struct value_table, value);
    all_redis_key_value_spec[2].nullbit_byte_offset = 0;
    all_redis_key_value_spec[2].nullbit_bit_in_byte = 0;

    entire_value_record = dict->createRecord(tab,
                                             all_redis_key_value_spec,
                                             3,
                                             sizeof(all_redis_key_value_spec[0]));
    if (entire_value_record == nullptr)
    {
        printf("Failed creating record for value table of STRING\n");
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
