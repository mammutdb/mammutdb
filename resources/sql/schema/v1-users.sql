CREATE TABLE mammutdb_users (
        id uuid DEFAULT uuid_generate_v1() UNIQUE PRIMARY KEY,
        username varchar(255) UNIQUE,
        password varchar(255) DEFAULT '!'
);
