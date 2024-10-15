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

#include "common.cc"
#include "string/commands.cc"


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
 */
