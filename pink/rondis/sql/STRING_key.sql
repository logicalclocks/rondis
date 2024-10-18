CREATE TABLE redis_string_keys(
    redis_key VARBINARY(3000) NOT NULL,
    -- We will use a ndb auto-increment
    -- This is to save space when referencing the key in the value table
    rondb_key BIGINT UNSIGNED,
    -- Max 512MiB
    value_data_type ENUM('string', 'number', 'binary_string') value_start VARBINARY(26500) NOT NULL,
    -- Max 512MiB --> 512 * 1,048,576 bytes = 536,870,912 characters
    -- --> To describe the length, one needs at least UINT (4,294,967,295)
    value_len INT UNSIGNED NOT NULL,
    value_start VARBINARY(26500) NOT NULL,
    -- Redis supports get/set of seconds/miliseconds
    expiry_date INT UNSIGNED,
    -- Easier to sort and delete keys this way
    KEY expiry_index(expiry_date),
    PRIMARY KEY (redis_key) USING HASH,
    UNIQUE KEY (rondb_key) USING HASH,
) ENGINE NDB -- Each CHAR will use 1 byte
CHARSET = latin1 COMMENT = "NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8"