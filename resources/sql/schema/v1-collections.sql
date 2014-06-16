CREATE TABLE mammutdb_collections (
       name varchar(1024) UNIQUE PRIMARY KEY,
       metadata json DEFAULT '{}'::json,
       created_at timestamp with time zone DEFAULT now()
);
