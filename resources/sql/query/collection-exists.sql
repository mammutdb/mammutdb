SELECT EXISTS (
       SELECT * FROM mammutdb_collections
       WHERE name = ?
);
