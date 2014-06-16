CREATE TABLE mammutdb_metadata (
       type varchar(255) UNIQUE PRIMARY KEY,
       value json DEFAULT '{}'::json
);
