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
#include "init.h"
#include "commands.h"

/**
 * Create NdbRecord's for all table accesses, they can be reused
 * for all Ndb objects.
 */
int init_key_record_specs(NdbDictionary::Dictionary *dict)
{
    const NdbDictionary::Table *tab = dict->getTable("redis_main_key");
    if (tab == nullptr)
    {
        printf("Failed getting table for key table of STRING\n");
        return -1;
    }
    const NdbDictionary::Column *key_val_col = tab->getColumn("key_val");
    const NdbDictionary::Column *key_id_col = tab->getColumn("key_id");
    const NdbDictionary::Column *expiry_date_col = tab->getColumn("expiry_date");
    const NdbDictionary::Column *value_col = tab->getColumn("value");
    const NdbDictionary::Column *tot_value_len_col = tab->getColumn("tot_value_len");
    const NdbDictionary::Column *num_rows_col = tab->getColumn("num_rows");
    const NdbDictionary::Column *row_state_col = tab->getColumn("row_state");
    const NdbDictionary::Column *tot_key_len_col = tab->getColumn("tot_key_len");

    if (key_val_col == nullptr ||
        key_id_col == nullptr ||
        expiry_date_col == nullptr ||
        value_col == nullptr ||
        tot_value_len_col == nullptr ||
        num_rows_col == nullptr ||
        row_state_col == nullptr ||
        tot_key_len_col == nullptr)
    {
        printf("Failed getting columns for key table of STRING\n");
        return -1;
    }

    NdbDictionary::RecordSpecification primary_redis_main_key_spec[1];
    NdbDictionary::RecordSpecification all_redis_main_key_spec[8];

    primary_redis_main_key_spec[0].column = key_val_col;
    primary_redis_main_key_spec[0].offset = offsetof(struct redis_main_key, key_val);
    primary_redis_main_key_spec[0].nullbit_byte_offset = 0;
    primary_redis_main_key_spec[0].nullbit_bit_in_byte = 0;
    primary_redis_main_key_record =
        dict->createRecord(tab,
                           primary_redis_main_key_spec,
                           1,
                           sizeof(primary_redis_main_key_spec[0]));
    if (primary_redis_main_key_record == nullptr)
    {
        printf("Failed creating record for key table of STRING\n");
        return -1;
    }

    all_redis_main_key_spec[0].column = key_val_col;
    all_redis_main_key_spec[0].offset = offsetof(struct redis_main_key, key_val);
    all_redis_main_key_spec[0].nullbit_byte_offset = 0;
    all_redis_main_key_spec[0].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[1].column = key_id_col;
    all_redis_main_key_spec[1].offset = offsetof(struct redis_main_key, key_id);
    all_redis_main_key_spec[1].nullbit_byte_offset = 0;
    all_redis_main_key_spec[1].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[2].column = expiry_date_col;
    all_redis_main_key_spec[2].offset = offsetof(struct redis_main_key, expiry_date);
    all_redis_main_key_spec[2].nullbit_byte_offset = 0;
    all_redis_main_key_spec[2].nullbit_bit_in_byte = 1;

    all_redis_main_key_spec[3].column = value_col;
    all_redis_main_key_spec[3].offset = offsetof(struct redis_main_key, value);
    all_redis_main_key_spec[3].nullbit_byte_offset = 0;
    all_redis_main_key_spec[3].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[4].column = tot_value_len_col;
    all_redis_main_key_spec[4].offset = offsetof(struct redis_main_key, tot_value_len);
    all_redis_main_key_spec[4].nullbit_byte_offset = 0;
    all_redis_main_key_spec[4].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[5].column = num_rows_col;
    all_redis_main_key_spec[5].offset = offsetof(struct redis_main_key, num_rows);
    all_redis_main_key_spec[5].nullbit_byte_offset = 0;
    all_redis_main_key_spec[5].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[6].column = row_state_col;
    all_redis_main_key_spec[6].offset = offsetof(struct redis_main_key, row_state);
    all_redis_main_key_spec[6].nullbit_byte_offset = 0;
    all_redis_main_key_spec[6].nullbit_bit_in_byte = 0;

    all_redis_main_key_spec[7].column = tot_key_len_col;
    all_redis_main_key_spec[7].offset = offsetof(struct redis_main_key, tot_key_len);
    all_redis_main_key_spec[7].nullbit_byte_offset = 0;
    all_redis_main_key_spec[7].nullbit_bit_in_byte = 0;

    all_redis_main_key_record = dict->createRecord(tab,
                                                   all_redis_main_key_spec,
                                                   8,
                                                   sizeof(all_redis_main_key_spec[0]));
    if (all_redis_main_key_record == nullptr)
    {
        printf("Failed creating record for key table of STRING\n");
        return -1;
    }
    return 0;
}

int init_value_record_specs(NdbDictionary::Dictionary *dict)
{
    const NdbDictionary::Table *tab = dict->getTable("redis_key_value");
    if (tab == nullptr)
    {
        printf("Failed getting table for value table of STRING\n");
        return -1;
    }
    const NdbDictionary::Column *key_id_col = tab->getColumn("key_id");
    const NdbDictionary::Column *ordinal_col = tab->getColumn("ordinal");
    const NdbDictionary::Column *value_col = tab->getColumn("value");
    if (key_id_col == nullptr ||
        ordinal_col == nullptr ||
        value_col == nullptr)
    {
        printf("Failed getting columns for value table of STRING\n");
        return -1;
    }

    NdbDictionary::RecordSpecification primary_redis_key_value_spec[2];
    NdbDictionary::RecordSpecification all_redis_key_value_spec[3];

    primary_redis_key_value_spec[0].column = key_id_col;
    primary_redis_key_value_spec[0].offset = offsetof(struct redis_key_value, key_id);
    primary_redis_key_value_spec[0].nullbit_byte_offset = 0;
    primary_redis_key_value_spec[0].nullbit_bit_in_byte = 0;

    primary_redis_key_value_spec[1].column = ordinal_col;
    primary_redis_key_value_spec[1].offset = offsetof(struct redis_key_value, ordinal);
    primary_redis_key_value_spec[1].nullbit_byte_offset = 0;
    primary_redis_key_value_spec[1].nullbit_bit_in_byte = 0;

    primary_redis_key_value_record = dict->createRecord(tab,
                                                        primary_redis_key_value_spec,
                                                        2,
                                                        sizeof(primary_redis_key_value_spec[0]));
    if (primary_redis_key_value_record == nullptr)
    {
        printf("Failed creating record for value table of STRING\n");
        return -1;
    }

    all_redis_key_value_spec[0].column = key_id_col;
    all_redis_key_value_spec[0].offset = offsetof(struct redis_key_value, key_id);
    all_redis_key_value_spec[0].nullbit_byte_offset = 0;
    all_redis_key_value_spec[0].nullbit_bit_in_byte = 0;

    all_redis_key_value_spec[1].column = ordinal_col;
    all_redis_key_value_spec[1].offset = offsetof(struct redis_key_value, ordinal);
    all_redis_key_value_spec[1].nullbit_byte_offset = 0;
    all_redis_key_value_spec[1].nullbit_bit_in_byte = 0;

    all_redis_key_value_spec[2].column = value_col;
    all_redis_key_value_spec[2].offset = offsetof(struct redis_key_value, value);
    all_redis_key_value_spec[2].nullbit_byte_offset = 0;
    all_redis_key_value_spec[2].nullbit_bit_in_byte = 0;

    all_redis_key_value_record = dict->createRecord(tab,
                                                    all_redis_key_value_spec,
                                                    3,
                                                    sizeof(all_redis_key_value_spec[0]));
    if (all_redis_key_value_record == nullptr)
    {
        printf("Failed creating record for value table of STRING\n");
        return -1;
    }

    return 0;
}