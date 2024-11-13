CREATE TABLE string_keys(
    -- Redis actually supports a max key size of 512MiB,
    -- but we choose not to support that here
    redis_key_id BIGINT UNSIGNED NOT NULL,
    redis_key VARBINARY(3000) NOT NULL,
    -- This is to save space when referencing the key in the value table
    rondb_key BIGINT UNSIGNED AUTO_INCREMENT NULL,
    -- TODO: Replace with Enum below
    value_data_type INT UNSIGNED NOT NULL,
    -- value_data_type ENUM('string', 'number', 'binary_string'),
    -- Max 512MiB --> 512 * 1,048,576 bytes = 536,870,912 characters
    -- --> To describe the length, one needs at least UINT (4,294,967,295)
    tot_value_len INT UNSIGNED NOT NULL,
    -- Technically implicit
    num_rows INT UNSIGNED NOT NULL,
    value_start VARBINARY(26500) NOT NULL,
    -- Redis supports get/set of seconds/milliseconds
    expiry_date INT UNSIGNED NOT NULL,
    -- Easier to sort and delete keys this way
    KEY expiry_index(expiry_date),
    PRIMARY KEY (redis_key_id, redis_key) USING HASH,
    UNIQUE KEY (rondb_key) USING HASH
) ENGINE NDB -- Each CHAR will use 1 byte
CHARSET = latin1 COMMENT = "NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8";
