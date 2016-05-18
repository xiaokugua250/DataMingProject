/*
CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] database_name
  [COMMENT database_comment]
  [LOCATION hdfs_path]
  [WITH DBPROPERTIES (property_name=property_value, ...)];*/

create DATABASE IF NOT EXISTS dmpDB
COMMENT "dmp project hive db"
LOCATION ${HIVE_HOME}
WITH DBPROPERTIES ("creator"="morty","data="2016-5-17");

--- describe database dmpDB;

-- DROP DATABASE
DROP DATABASE IF_EXISTES dmpDB ;

--- ALTER DATABASE
/*

ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...);   -- (Note: SCHEMA added in Hive 0.14.0)

ALTER (DATABASE|SCHEMA) database_name SET OWNER [USER|ROLE] user_or_role;
*/

ALTER DATABASE dmpDB SET DBPROPERTIES("creator"="morty","edit_by"="morty");
ALTER DATABASE dmpDB SET OWNER USER morty;

USE dmpDB;
USE DEFAULT;