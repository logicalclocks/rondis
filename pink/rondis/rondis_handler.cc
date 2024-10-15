/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#define MAX_CONNECTIONS 1
#define MAX_NDB_PER_CONNECTION 1

#define INLINE_VALUE_LEN 26500
#define EXTENSION_VALUE_LEN 29500
#define MAX_KEY_VALUE_LEN 3000

#define FOREIGN_KEY_RESTRICT_ERROR 256

struct redis_main_key
{
    Uint32 null_bits;
    char key_val[MAX_KEY_VALUE_LEN + 2];
    Uint64 key_id;
    Uint32 expiry_date;
    Uint32 tot_value_len;
    Uint32 num_rows;
    Uint32 row_state;
    Uint32 tot_key_len;
    char value[INLINE_VALUE_LEN + 2];
};

struct redis_key_value
{
    Uint64 key_id;
    Uint32 ordinal;
    char value[EXTENSION_VALUE_LEN];
};

struct redis_main_field
{
    Uint32 null_bits;
    char field_name[MAX_KEY_VALUE_LEN + 2];
    Uint64 key_id;
    Uint64 field_id;
    Uint32 num_value_rows;
    Uint32 tot_value_len;
    Uint32 tot_key_len;
    char value[INLINE_VALUE_LEN + 2];
};

struct redis_field_value
{
    Uint64 field_id;
    Uint32 ordinal;
    char value[EXTENSION_VALUE_LEN + 2];
};

void rondb_get_command(pink::RedisCmdArgsType &,
                       std::string *response,
                       int fd);
void rondb_set_command(pink::RedisCmdArgsType &,
                       std::string *response,
                       int fd);
Ndb_cluster_connection *rondb_conn[MAX_CONNECTIONS];
Ndb *rondb_ndb[MAX_CONNECTIONS][MAX_NDB_PER_CONNECTION];

NdbDictionary::RecordSpecification primary_redis_main_key_spec[1];
NdbDictionary::RecordSpecification all_redis_main_key_spec[8];

NdbDictionary::RecordSpecification primary_redis_key_value_spec[2];
NdbDictionary::RecordSpecification all_redis_key_value_spec[3];

NdbDictionary::RecordSpecification primary_redis_main_field_spec[2];
NdbDictionary::RecordSpecification all_redis_main_field_spec[7];

NdbDictionary::RecordSpecification primary_redis_field_value_spec[2];
NdbDictionary::RecordSpecification all_redis_field_value_spec[3];

NdbRecord *primary_redis_main_key_record = nullptr;
NdbRecord *all_redis_main_key_record = nullptr;
NdbRecord *primary_redis_key_value_record = nullptr;
NdbRecord *all_redis_key_value_record = nullptr;
NdbRecord *primary_redis_main_field_record = nullptr;
NdbRecord *all_redis_main_field_record = nullptr;
NdbRecord *primary_redis_field_value_record = nullptr;
NdbRecord *all_redis_field_value_record = nullptr;

int write_formatted(char *buffer, int bufferSize, const char *format, ...)
{
    int len = 0;
    va_list arguments;
    va_start(arguments, format);
    len = vsnprintf(buffer, bufferSize, format, arguments);
    va_end(arguments);
    return len;
}

void append_response(std::string *response, const char *app_str, Uint32 error_code)
{
    char buf[512];
    printf("Add %s to response, error: %u\n", app_str, error_code);
    if (error_code == 0)
    {
        write_formatted(buf, sizeof(buf), "-%s\r\n", app_str);
        response->append(app_str);
    }
    else
    {
        write_formatted(buf, sizeof(buf), "-%s: %u\r\n", app_str, error_code);
        response->append(app_str);
    }
}

void failed_no_such_row_error(std::string *response)
{
    response->append("$-1\r\n");
}

void failed_read_error(std::string *response, Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to read key, code:",
                    error_code);
}

void failed_create_table(std::string *response, Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to create table object",
                    error_code);
}

void failed_create_transaction(std::string *response,
                               Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to create transaction object",
                    error_code);
}

void failed_execute(std::string *response, Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to execute transaction, code:",
                    error_code);
}

void failed_get_operation(std::string *response)
{
    response->append("-RonDB Error: Failed to get NdbOperation object\r\n");
}

void failed_define(std::string *response, Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to define RonDB operation, code:",
                    error_code);
}

void failed_large_key(std::string *response)
{
    response->append("-RonDB Error: Support up to 3000 bytes long keys\r\n");
}

int rondb_connect(const char *connect_string,
                  unsigned int num_connections)
{
    ndb_init();
    for (unsigned int i = 0; i < MAX_CONNECTIONS; i++)
    {
        rondb_conn[i] = new Ndb_cluster_connection(connect_string);
        if (rondb_conn[i]->connect() != 0)
        {
            printf("Kilroy C\n");
            return -1;
        }
        printf("Connected to cluster\n");
        if (rondb_conn[i]->wait_until_ready(30, 0) != 0)
        {
            printf("Kilroy CI\n");
            return -1;
        }
        printf("Connected to started cluster\n");
        for (unsigned int j = 0; j < MAX_NDB_PER_CONNECTION; j++)
        {
            Ndb *ndb = new Ndb(rondb_conn[i], "redis_0");
            if (ndb == nullptr)
            {
                printf("Kilroy CII\n");
                return -1;
            }
            if (ndb->init() != 0)
            {
                printf("Kilroy CIII\n");
                return -1;
            }
            rondb_ndb[i][j] = ndb;
        }
    }
    /**
     * Create NdbRecord's for all table accesses, they can be reused
     * for all Ndb objects.
     */
    Ndb *ndb = rondb_ndb[0][0];
    NdbDictionary::Dictionary *dict = ndb->getDictionary();
    {
        const NdbDictionary::Table *tab = dict->getTable("redis_main_key");
        if (tab == nullptr)
        {
            printf("Kilroy XX\n");
            return -1;
        }
        const NdbDictionary::Column *key_val_col = tab->getColumn("key_val");
        const NdbDictionary::Column *key_id_col = tab->getColumn("key_id");
        const NdbDictionary::Column *expiry_date_col =
            tab->getColumn("expiry_date");
        const NdbDictionary::Column *value_col =
            tab->getColumn("value");
        const NdbDictionary::Column *tot_value_len_col =
            tab->getColumn("tot_value_len");
        const NdbDictionary::Column *num_rows_col =
            tab->getColumn("num_rows");
        const NdbDictionary::Column *row_state_col =
            tab->getColumn("row_state");
        const NdbDictionary::Column *tot_key_len_col =
            tab->getColumn("tot_key_len");

        if (key_val_col == nullptr ||
            key_id_col == nullptr ||
            expiry_date_col == nullptr ||
            value_col == nullptr ||
            tot_value_len_col == nullptr ||
            num_rows_col == nullptr ||
            row_state_col == nullptr ||
            tot_key_len_col == nullptr)
        {
            printf("Kilroy XXI\n");
            return -1;
        }

        primary_redis_main_key_spec[0].column = key_val_col;
        primary_redis_main_key_spec[0].offset =
            offsetof(struct redis_main_key, key_val);
        primary_redis_main_key_spec[0].nullbit_byte_offset = 0;
        primary_redis_main_key_spec[0].nullbit_bit_in_byte = 0;
        primary_redis_main_key_record =
            dict->createRecord(tab,
                               primary_redis_main_key_spec,
                               1,
                               sizeof(primary_redis_main_key_spec[0]));
        if (primary_redis_main_key_record == nullptr)
        {
            printf("Kilroy XXII\n");
            return -1;
        }

        all_redis_main_key_spec[0].column = key_val_col;
        all_redis_main_key_spec[0].offset =
            offsetof(struct redis_main_key, key_val);
        all_redis_main_key_spec[0].nullbit_byte_offset = 0;
        all_redis_main_key_spec[0].nullbit_bit_in_byte = 0;

        all_redis_main_key_spec[1].column = key_id_col;
        all_redis_main_key_spec[1].offset =
            offsetof(struct redis_main_key, key_id);
        all_redis_main_key_spec[1].nullbit_byte_offset = 0;
        all_redis_main_key_spec[1].nullbit_bit_in_byte = 0;

        all_redis_main_key_spec[2].column = expiry_date_col;
        all_redis_main_key_spec[2].offset =
            offsetof(struct redis_main_key, expiry_date);
        all_redis_main_key_spec[2].nullbit_byte_offset = 0;
        all_redis_main_key_spec[2].nullbit_bit_in_byte = 1;

        all_redis_main_key_spec[3].column = value_col;
        all_redis_main_key_spec[3].offset =
            offsetof(struct redis_main_key, value);
        all_redis_main_key_spec[3].nullbit_byte_offset = 0;
        all_redis_main_key_spec[3].nullbit_bit_in_byte = 0;

        all_redis_main_key_spec[4].column = tot_value_len_col;
        all_redis_main_key_spec[4].offset =
            offsetof(struct redis_main_key, tot_value_len);
        all_redis_main_key_spec[4].nullbit_byte_offset = 0;
        all_redis_main_key_spec[4].nullbit_bit_in_byte = 0;

        all_redis_main_key_spec[5].column = num_rows_col;
        all_redis_main_key_spec[5].offset =
            offsetof(struct redis_main_key, num_rows);
        all_redis_main_key_spec[5].nullbit_byte_offset = 0;
        all_redis_main_key_spec[5].nullbit_bit_in_byte = 0;

        all_redis_main_key_spec[6].column = row_state_col;
        all_redis_main_key_spec[6].offset =
            offsetof(struct redis_main_key, row_state);
        all_redis_main_key_spec[6].nullbit_byte_offset = 0;
        all_redis_main_key_spec[6].nullbit_bit_in_byte = 0;

        all_redis_main_key_spec[7].column = tot_key_len_col;
        all_redis_main_key_spec[7].offset =
            offsetof(struct redis_main_key, tot_key_len);
        all_redis_main_key_spec[7].nullbit_byte_offset = 0;
        all_redis_main_key_spec[7].nullbit_bit_in_byte = 0;

        all_redis_main_key_record =
            dict->createRecord(tab,
                               all_redis_main_key_spec,
                               8,
                               sizeof(all_redis_main_key_spec[0]));
        if (all_redis_main_key_record == nullptr)
        {
            printf("Kilroy XXIII\n");
            return -1;
        }
    }

    {
        const NdbDictionary::Table *tab = dict->getTable("redis_key_value");
        if (tab == nullptr)
        {
            printf("Kilroy XXIV\n");
            return -1;
        }
        const NdbDictionary::Column *key_id_col = tab->getColumn("key_id");
        const NdbDictionary::Column *ordinal_col = tab->getColumn("ordinal");
        const NdbDictionary::Column *value_col = tab->getColumn("value");
        if (key_id_col == nullptr ||
            ordinal_col == nullptr ||
            value_col == nullptr)
        {
            printf("Kilroy XXV\n");
            return -1;
        }
        primary_redis_key_value_spec[0].column = key_id_col;
        primary_redis_key_value_spec[0].offset =
            offsetof(struct redis_key_value, key_id);
        primary_redis_key_value_spec[0].nullbit_byte_offset = 0;
        primary_redis_key_value_spec[0].nullbit_bit_in_byte = 0;

        primary_redis_key_value_spec[1].column = ordinal_col;
        primary_redis_key_value_spec[1].offset =
            offsetof(struct redis_key_value, ordinal);
        primary_redis_key_value_spec[1].nullbit_byte_offset = 0;
        primary_redis_key_value_spec[1].nullbit_bit_in_byte = 0;

        primary_redis_key_value_record =
            dict->createRecord(tab,
                               primary_redis_key_value_spec,
                               2,
                               sizeof(primary_redis_key_value_spec[0]));
        if (primary_redis_key_value_record == nullptr)
        {
            printf("Kilroy XXVI\n");
            return -1;
        }

        all_redis_key_value_spec[0].column = key_id_col;
        all_redis_key_value_spec[0].offset =
            offsetof(struct redis_key_value, key_id);
        all_redis_key_value_spec[0].nullbit_byte_offset = 0;
        all_redis_key_value_spec[0].nullbit_bit_in_byte = 0;

        all_redis_key_value_spec[1].column = ordinal_col;
        all_redis_key_value_spec[1].offset =
            offsetof(struct redis_key_value, ordinal);
        all_redis_key_value_spec[1].nullbit_byte_offset = 0;
        all_redis_key_value_spec[1].nullbit_bit_in_byte = 0;

        all_redis_key_value_spec[2].column = value_col;
        all_redis_key_value_spec[2].offset =
            offsetof(struct redis_key_value, value);
        all_redis_key_value_spec[2].nullbit_byte_offset = 0;
        all_redis_key_value_spec[2].nullbit_bit_in_byte = 0;

        all_redis_key_value_record =
            dict->createRecord(tab,
                               all_redis_key_value_spec,
                               3,
                               sizeof(all_redis_key_value_spec[0]));
        if (all_redis_key_value_record == nullptr)
        {
            printf("Kilroy XXVII\n");
            return -1;
        }
    }

    {
        const NdbDictionary::Table *tab = dict->getTable("redis_main_field");
        if (tab == nullptr)
        {
            printf("Kilroy eXVIII\n");
            return -1;
        }
        const NdbDictionary::Column *key_id_col = tab->getColumn("key_id");
        const NdbDictionary::Column *field_name_col =
            tab->getColumn("field_name");
        const NdbDictionary::Column *field_id_col =
            tab->getColumn("field_id");
        const NdbDictionary::Column *value_col = tab->getColumn("value");
        const NdbDictionary::Column *num_value_rows_col =
            tab->getColumn("num_value_rows");
        const NdbDictionary::Column *tot_value_len_col =
            tab->getColumn("tot_value_len");
        const NdbDictionary::Column *tot_key_len_col =
            tab->getColumn("tot_key_len");

        if (key_id_col == nullptr ||
            field_name_col == nullptr ||
            field_id_col == nullptr ||
            value_col == nullptr ||
            num_value_rows_col == nullptr ||
            tot_value_len_col == nullptr ||
            tot_key_len_col == nullptr)
        {
            printf("Kilroy XXIX\n");
            return -1;
        }
        printf("Kilroy IV Was here\n");

        primary_redis_main_field_spec[0].column = key_id_col;
        primary_redis_main_field_spec[0].offset =
            offsetof(struct redis_main_field, key_id);
        primary_redis_main_field_spec[0].nullbit_byte_offset = 0;
        primary_redis_main_field_spec[0].nullbit_bit_in_byte = 0;

        primary_redis_main_field_spec[1].column = field_name_col;
        primary_redis_main_field_spec[1].offset =
            offsetof(struct redis_main_field, field_name);
        primary_redis_main_field_spec[1].nullbit_byte_offset = 0;
        primary_redis_main_field_spec[1].nullbit_bit_in_byte = 0;

        primary_redis_main_field_record =
            dict->createRecord(tab,
                               primary_redis_main_field_spec,
                               1,
                               sizeof(primary_redis_main_field_spec[0]));
        if (primary_redis_main_field_record == nullptr)
        {
            printf("Kilroy XXX\n");
            return -1;
        }
        printf("Kilroy V Was here\n");

        all_redis_main_field_spec[0].column = key_id_col;
        all_redis_main_field_spec[0].offset =
            offsetof(struct redis_main_field, key_id);
        all_redis_main_field_spec[0].nullbit_byte_offset = 0;
        all_redis_main_field_spec[0].nullbit_bit_in_byte = 0;

        all_redis_main_field_spec[1].column = field_name_col;
        all_redis_main_field_spec[1].offset =
            offsetof(struct redis_main_field, field_name);
        all_redis_main_field_spec[1].nullbit_byte_offset = 0;
        all_redis_main_field_spec[1].nullbit_bit_in_byte = 0;

        all_redis_main_field_spec[2].column = field_id_col;
        all_redis_main_field_spec[2].offset =
            offsetof(struct redis_main_field, field_id);
        all_redis_main_field_spec[2].nullbit_byte_offset = 0;
        all_redis_main_field_spec[2].nullbit_bit_in_byte = 0;

        all_redis_main_field_spec[3].column = value_col;
        all_redis_main_field_spec[3].offset =
            offsetof(struct redis_main_field, value);
        all_redis_main_field_spec[3].nullbit_byte_offset = 0;
        all_redis_main_field_spec[3].nullbit_bit_in_byte = 0;

        all_redis_main_field_spec[4].column = num_value_rows_col;
        all_redis_main_field_spec[4].offset =
            offsetof(struct redis_main_field, num_value_rows);
        all_redis_main_field_spec[4].nullbit_byte_offset = 0;
        all_redis_main_field_spec[4].nullbit_bit_in_byte = 0;

        all_redis_main_field_spec[5].column = tot_value_len_col;
        all_redis_main_field_spec[5].offset =
            offsetof(struct redis_main_field, tot_value_len);
        all_redis_main_field_spec[5].nullbit_byte_offset = 0;
        all_redis_main_field_spec[5].nullbit_bit_in_byte = 0;

        all_redis_main_field_spec[6].column = tot_key_len_col;
        all_redis_main_field_spec[6].offset =
            offsetof(struct redis_main_field, tot_key_len);
        all_redis_main_field_spec[6].nullbit_byte_offset = 0;
        all_redis_main_field_spec[6].nullbit_bit_in_byte = 0;

        all_redis_main_field_record =
            dict->createRecord(tab,
                               all_redis_main_field_spec,
                               7,
                               sizeof(all_redis_main_field_spec[0]));
        if (all_redis_main_field_record == nullptr)
        {
            printf("Kilroy XXXI\n");
            return -1;
        }
        printf("Kilroy VI Was here\n");
    }
    {
        const NdbDictionary::Table *tab = dict->getTable("redis_field_value");
        if (tab == nullptr)
        {
            printf("Kilroy XXXII\n");
            return -1;
        }

        printf("Kilroy VIII Was here\n");
        const NdbDictionary::Column *field_id_col = tab->getColumn("field_id");
        const NdbDictionary::Column *ordinal_col = tab->getColumn("ordinal");
        const NdbDictionary::Column *value_col = tab->getColumn("value");
        if (field_id_col == nullptr ||
            ordinal_col == nullptr ||
            value_col == nullptr)
        {
            printf("Kilroy XXXIII\n");
            return -1;
        }
        primary_redis_field_value_spec[0].column = field_id_col;
        primary_redis_field_value_spec[0].offset =
            offsetof(struct redis_field_value, field_id);
        primary_redis_field_value_spec[0].nullbit_byte_offset = 0;
        primary_redis_field_value_spec[0].nullbit_bit_in_byte = 0;

        primary_redis_field_value_record =
            dict->createRecord(tab,
                               primary_redis_field_value_spec,
                               1,
                               sizeof(primary_redis_field_value_spec[0]));
        if (primary_redis_field_value_record == nullptr)
        {
            printf("Kilroy XXXIV\n");
            return -1;
        }
        printf("Kilroy VII Was here\n");

        all_redis_field_value_spec[0].column = field_id_col;
        all_redis_field_value_spec[0].offset =
            offsetof(struct redis_field_value, field_id);
        all_redis_field_value_spec[0].nullbit_byte_offset = 0;
        all_redis_field_value_spec[0].nullbit_bit_in_byte = 0;

        all_redis_field_value_spec[1].column = ordinal_col;
        all_redis_field_value_spec[1].offset =
            offsetof(struct redis_field_value, ordinal);
        all_redis_field_value_spec[1].nullbit_byte_offset = 0;
        all_redis_field_value_spec[1].nullbit_bit_in_byte = 0;

        all_redis_field_value_spec[2].column = value_col;
        all_redis_field_value_spec[2].offset =
            offsetof(struct redis_field_value, value);
        all_redis_field_value_spec[2].nullbit_byte_offset = 0;
        all_redis_field_value_spec[2].nullbit_bit_in_byte = 0;

        all_redis_field_value_record =
            dict->createRecord(tab,
                               all_redis_field_value_spec,
                               3,
                               sizeof(all_redis_field_value_spec[0]));
        if (all_redis_field_value_record == nullptr)
        {
            printf("Kilroy XXXV\n");
            return -1;
        }
    }
    printf("Kilroy came here I\n");
    return 0;
}

void rondb_end()
{
    ndb_end(0);
}

int rondb_redis_handler(pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int fd)
{
    if (argv.size() == 0)
    {
        return -1;
    }
    const char *cmd_str = argv[0].c_str();
    unsigned int cmd_len = strlen(cmd_str);
    if (cmd_len == 3)
    {
        const char *set_str = "set";
        const char *get_str = "get";
        if (memcmp(cmd_str, get_str, 3) == 0)
        {
            rondb_get_command(argv, response, fd);
        }
        else if (memcmp(cmd_str, set_str, 3) == 0)
        {
            rondb_set_command(argv, response, fd);
        }
        return 0;
    }
    else if (cmd_len == 1)
    {
        const char *shutdown_str = "shutdown";
        if (memcmp(cmd_str, shutdown_str, 8) == 0)
        {
            printf("Shutdown Rondis server\n");
            return -1;
        }
    }
    return -1;
}

/**
 * Mapping the Redis commands to RonDB requests is done
 * in the following manner.
 *
 * Each database in Redis is a separate table belonging
 * to a database. In Redis the commands are sent to the
 * current database selected by the SELECT command.
 * So e.g. SELECT 0 will select database 0, thus the
 * table name of Redis tables in RonDB will always be
 * Redis, but the database will be "0" or the number of
 * the database used in Redis.
 *
 * All Redis tables will have the same format.
 CREATE TABLE redis_main_key(
   key_val VARBINARY(3000) NOT NULL,
   key_id BIGINT UNSIGNED,
   expiry_date INT UNSIGNED,
   value VARBINARY(26500) NOT NULL,
   tot_value_len INT UNSIGNED NOT NULL,
   num_rows INT UNSIGNED NOT NULL,
   row_state INT UNSIGNED NOT NULL,
   tot_key_len INT UNSIGNED NOT NULL,
   PRIMARY KEY (key_val) USING HASH,
   UNIQUE KEY (key_id) USING HASH,
   KEY expiry_index(expiry_date))
   ENGINE NDB
   CHARSET=latin1
   COMMENT="NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8"
 *
 * The redis_main table is the starting point for key-value
 * objects, for hashes and other data structures in Redis.
 * The current object is always using version_id = 0.
 * When a row has expired, but it is still required to be
 * around, then the version_id is set to the key_id.
 *
 * We have an ordered index on expiry_date, by scanning
 * this from the oldest value we quickly find keys that
 * require deletion. The expiry_date column is also used
 * to figure out whether a key value actually exists or
 * not.
 *
 * If value_rows is 0, there are no value rows attached.
 * Otherwise it specifies the number of value rows.
 *
 * The this_value_len specifies the number of bytes of
 * values stored in this row. The tot_value_len specifies
 * the total number of value bytes of the key.
 *
 * For hash keys the fields are stored in their own rows.
 * The field_rows specifies the number of fields this key
 * has. It must be a number bigger than 0. If there are
 * fields in the key there is no value in the key, so
 * obviously also requires value_rows to be 0.
 *
 * The row_state contains information about data type of
 * the key, of the value, whether we have all fields
 * inlined in the values object and whether the row is
 * expired and deletion process is ongoing.
 *
 * Bit 0-1 in row_state is the data type of the key.
 * 0 means a string
 * 1 means a number
 * 2 means a binary string
 *
 * Bit 2-3 in row_state is the data type of the value.
 *
 * The key_id is a unique reference of the row that is
 * never reused (it is a 64 bit value and should last
 * for 100's of years). The key_id removes the need to
 * store the full key value in multiple tables.
 *
 * The value extensions are stored in a separate table
 * for keys which have the following format:
 *
  CREATE TABLE redis_key_value(
    key_id BIGINT UNSIGNED NOT NULL,
    ordinal INT UNSIGNED NOT NULL,
    value VARBINARY(29500) NOT NULL,
    PRIMARY KEY (key_id, ordinal),
    FOREIGN KEY (key_id)
     REFERENCES redis_main_key(key_id)
     ON UPDATE RESTRICT ON DELETE CASCADE)
    ENGINE NDB,
    COMMENT="NDB_TABLE=PARTITION_BALANCE=RP_BY_LDM_X_8"
    PARTITION BY KEY (key_id)
 *
 * For the data type Hash we will use another separate
 * table to store the field values. Each field value
 * will have some value data stored inline, but could
 * also have parts of the value stored in the redis_ext_value
 * table. The field_id is a unique identifier that is
 * referencing the redis_ext_value table.
 *
  CREATE TABLE redis_main_field(
    key_id BIGINT UNSIGNED NOT NULL,
    field_name VARBINARY(3000) NOT NULL,
    field_id BIGINT UNSIGNED,
    value VARBINARY(26500) NOT NULL,
    num_value_rows INT UNSIGNED NOT NULL,
    tot_value_len INT UNSIGNED NOT NULL,
    tot_key_len INT UNSIGNED NOT NULL,
    PRIMARY KEY (key_id, field_name) USING HASH,
    UNIQUE KEY (field_id) USING HASH)
    ENGINE NDB
    COMMENT="NDB_TABLE=PARTITION_BALANCE=RP_BY_LDM_X_8"
 *
 * The value extensions are stored in a separate table
 * for keys which have the following format:
  CREATE TABLE redis_field_value(
    field_id BIGINT UNSIGNED NOT NULL,
    ordinal INT UNSIGNED NOT NULL,
    value VARBINARY(29500) NOT NULL,
    PRIMARY KEY (field_id, ordinal),
    FOREIGN KEY (field_id)
     REFERENCES redis_main_field(field_id)
     ON UPDATE RESTRICT ON DELETE CASCADE)
    ENGINE NDB,
    COMMENT="NDB_TABLE=PARTITION_BALANCE=RP_BY_LDM_X_8"
    PARTITION BY KEY (field_id)
 *
 * For most rows the key will fit in the key field and
 * the value will fit in the value field. In this case
 * row in Redis maps directly to a row in RonDB. However
 * for large rows with large values, several RonDB rows
 * might be required. The extended rows will be stored
 * ordinal set to 0, 1, 2, 3 and so forth. The first row
 * will have ordinal set to 0. Since we have an ordered
 * index on key, we can find all rows using a partition
 * pruned index scan on the key field. We also use key
 * as the partition key to ensure these scans are
 * partition pruned index scans. It is also possible to
 * use batched primary key lookups to get the value rows.
 *
 * By using a Read lock on the primary key lookup of
 * the first row, we can ensure that the read of the
 * entire row is stable since the row cannot be changed
 * without having an exclusive lock on the first row.
 *
 * As an optimisation GET operations will first perform
 * a READ COMMITTED, this will suffice if there are no
 * value rows and there are no fields. For hash keys this
 * optimisation isn't used.
 */
int execute_no_commit(NdbTransaction *trans, int &ret_code, bool allow_fail)
{
    printf("Execute NoCommit\n");
    if (trans->execute(NdbTransaction::NoCommit) != 0)
    {
        ret_code = trans->getNdbError().code;
        return -1;
    }
    return 0;
}

int execute_commit(Ndb *ndb, NdbTransaction *trans, int &ret_code)
{
    printf("Execute transaction\n");
    if (trans->execute(NdbTransaction::Commit) != 0)
    {
        ret_code = trans->getNdbError().code;
        return -1;
    }
    ndb->closeTransaction(trans);
    return 0;
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

#define READ_VALUE_ROWS 1
#define RONDB_INTERNAL_ERROR 2
#define READ_ERROR 626

int get_simple_key_row(std::string *response,
                       const NdbDictionary::Table *tab,
                       Ndb *ndb,
                       struct redis_main_key *row,
                       Uint32 key_len)
{
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
        primary_redis_main_key_record,
        (const char *)row,
        all_redis_main_key_record,
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
        printf("Respond with len: %u, %u tot_value_len, string: %s, string_len: %u\n", len, row->tot_value_len, response->c_str(), response->length());
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
    struct redis_key_value row[2];
    row[0].key_id = key_id;
    row[1].key_id = key_id;
    Uint32 row_index = 0;
    for (Uint32 index = 0; index < num_rows; index++)
    {
        row[row_index].ordinal = index;
        const NdbOperation *read_op = trans->readTuple(
            primary_redis_key_value_record,
            (const char *)&row,
            all_redis_key_value_record,
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
                        struct redis_main_key *row,
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
        primary_redis_main_key_record,
        (const char *)row,
        all_redis_main_key_record,
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

void rondb_get_command(pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int fd)
{
    if (argv.size() < 2)
    {
        return;
    }
    const char *key_str = argv[1].c_str();
    Uint32 key_len = argv[1].size();
    if (key_len > MAX_KEY_VALUE_LEN)
    {
        failed_large_key(response);
        return;
    }
    Ndb *ndb = rondb_ndb[0][0];
    const NdbDictionary::Dictionary *dict = ndb->getDictionary();
    const NdbDictionary::Table *tab = dict->getTable("redis_main_key");
    if (tab == nullptr)
    {
        failed_create_table(response, dict->getNdbError().code);
        return;
    }
    struct redis_main_key row_object;
    char key_buf[MAX_KEY_VALUE_LEN + 2];
    memcpy(&row_object.key_val[2], key_str, key_len);
    row_object.key_val[0] = key_len & 255;
    row_object.key_val[1] = key_len >> 8;
    {
        int ret_code = get_simple_key_row(response,
                                          tab,
                                          ndb,
                                          &row_object,
                                          key_len);
        if (ret_code == 0)
        {
            /* Row found and read, return result */
            return;
        }
        else if (ret_code == READ_ERROR)
        {
            /* Row not found, return error */
            return;
        }
        else if (ret_code == READ_VALUE_ROWS)
        {
            /* Row uses value rows, so more complex read is required */
            ret_code = get_complex_key_row(response,
                                           dict,
                                           tab,
                                           ndb,
                                           &row_object,
                                           key_len);
            if (ret_code == 0)
            {
                /* Rows found and read, return result */
                return;
            }
            else if (ret_code == READ_ERROR)
            {
                /* Row not found, return error */
                return;
            }
        }
    }
    /* Some RonDB occurred, already created response */
    return;
}

void rondb_set_command(pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int fd)
{
    printf("Kilroy came here II\n");
    if (argv.size() < 3)
    {
        append_response(response, "ERR Too few arguments in SET command", 0);
        return;
    }
    Ndb *ndb = rondb_ndb[0][0];
    const char *key_str = argv[1].c_str();
    Uint32 key_len = argv[1].size();
    const char *value_str = argv[2].c_str();
    Uint32 value_len = argv[2].size();
    if (key_len > MAX_KEY_VALUE_LEN)
    {
        failed_large_key(response);
        return;
    }
    const NdbDictionary::Dictionary *dict = ndb->getDictionary();
    const NdbDictionary::Table *tab = dict->getTable("redis_main_key");
    if (tab == nullptr)
    {
        failed_create_table(response, dict->getNdbError().code);
        return;
    }
    printf("Kilroy came here III\n");
    NdbTransaction *trans = ndb->startTransaction(tab, key_str, key_len);
    if (trans == nullptr)
    {
        failed_create_transaction(response, ndb->getNdbError().code);
        return;
    }
    char varsize_param[EXTENSION_VALUE_LEN + 500];
    Uint32 value_rows = 0;
    Uint64 key_id = 0;
    if (value_len > INLINE_VALUE_LEN)
    {
        /**
         * The row doesn't fit in one RonDB row, create more rows
         * in the redis_key_values table.
         *
         * We also use the generated key_id which is the foreign
         * key column in the redis_main_key table such that
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
            int ret_code = create_key_value_row(response,
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
                    failed_execute(response, ret_code);
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