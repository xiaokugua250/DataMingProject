/*
CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name    -- (Note: TEMPORARY available in Hive 0.14.0 and later)
  [(col_name data_type [COMMENT col_comment], ...)]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
  [CLUSTERED BY (col_name, col_name, ...) [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]
  [SKEWED BY (col_name, col_name, ...)                  -- (Note: Available in Hive 0.10.0 and later)]
     ON ((col_value, col_value, ...), (col_value, col_value, ...), ...)
     [STORED AS DIRECTORIES]
  [
   [ROW FORMAT row_format]
   [STORED AS file_format]
     | STORED BY 'storage.handler.class.name' [WITH SERDEPROPERTIES (...)]  -- (Note: Available in Hive 0.6.0 and later)
  ]
  [LOCATION hdfs_path]
  [TBLPROPERTIES (property_name=property_value, ...)]   -- (Note: Available in Hive 0.6.0 and later)
  [AS select_statement];   -- (Note: Available in Hive 0.5.0 and later; not supported for external tables)

CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
  LIKE existing_table_or_view_name
  [LOCATION hdfs_path];

data_type
  : primitive_type
  | array_type
  | map_type
  | struct_type
  | union_type  -- (Note: Available in Hive 0.7.0 and later)

primitive_type
  : TINYINT
  | SMALLINT
  | INT
  | BIGINT
  | BOOLEAN
  | FLOAT
  | DOUBLE
  | STRING
  | BINARY      -- (Note: Available in Hive 0.8.0 and later)
  | TIMESTAMP   -- (Note: Available in Hive 0.8.0 and later)
  | DECIMAL     -- (Note: Available in Hive 0.11.0 and later)
  | DECIMAL(precision, scale)  -- (Note: Available in Hive 0.13.0 and later)
  | DATE        -- (Note: Available in Hive 0.12.0 and later)
  | VARCHAR     -- (Note: Available in Hive 0.12.0 and later)
  | CHAR        -- (Note: Available in Hive 0.13.0 and later)

array_type
  : ARRAY < data_type >

map_type
  : MAP < primitive_type, data_type >

struct_type
  : STRUCT < col_name : data_type [COMMENT col_comment], ...>

union_type
   : UNIONTYPE < data_type, data_type, ... >  -- (Note: Available in Hive 0.7.0 and later)

row_format
  : DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]] [COLLECTION ITEMS TERMINATED BY char]
        [MAP KEYS TERMINATED BY char] [LINES TERMINATED BY char]
        [NULL DEFINED AS char]   -- (Note: Available in Hive 0.13 and later)
  | SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, property_name=property_value, ...)]

file_format:
  : SEQUENCEFILE
  | TEXTFILE    -- (Default, depending on hive.default.fileformat configuration)
  | RCFILE      -- (Note: Available in Hive 0.6.0 and later)
  | ORC         -- (Note: Available in Hive 0.11.0 and later)
  | PARQUET     -- (Note: Available in Hive 0.13.0 and later)
  | AVRO        -- (Note: Available in Hive 0.14.0 and later)
  | INPUTFORMAT input_format_classname OUTPUTFORMAT output_format_classname
*/

--创建表
CREATE TABLE IF NOT EXISTS dmp_useraccount(
userid BINGINT COMMENT "userid",
userpassword  STRING COMMENT "userpassword",
userpasswordmd5 STRING COMMENT "userpasswordencryption"
useremail   STRING COMMENT "useremail",
) COMMENT "user account table"
PARTIONED BY(rgtime STRING)
FIELDS TERMINATED BY '\001'
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
STORED AS TEXTFILE;



--创建外部表
CREATE  EXTERNAL TABLE  IF NOT EXISTS dmpbean_user(
    userid INT COMMENT "user_id",
    username STRING COMMENT "user_name",
    userage SMALLINT COMMENT "user_age",
    userbirth DATE  COMMENT "userbirthday",
    userfamliy MAP<STRING,STRING> COMMENT "userfamilyinfo",
    useremail ARRAY<STRING>  COMMENT "useremail",
    userincome STRING COMMENT "user_income",
    userdescribe STRING COMMENT "user_describe",
    userpostcode INT COMMENT "user_postcode",
    userAddress STRUCT <province:STRING COMMENT "user_province",city:STRING COMMENT "user_city",street:STRING COMMENT "user_street"> COMMNET "user_address",

) COMMENT "USER BEAN TABLE DEMO"
PARTITIONED BY(province STRING,city STRING)
CLUSTERED BY(userage) INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'   --[ROW FORMAT DELIMITED]关键字，是用来设置创建的表在加载数据的时候，支持的列分隔符。不同列之间用一个'\001'分割,集合(例如array,map)的元素之间以'\002'隔开,map中key和value用'\003'分割。
STORED AS TEXTFILE    --不同的文件格式
LOCATION "hdfs://dev/data"  --数据位置
;

-- Create Table As Select(CTAS)
/*
The target table cannot be a partitioned table.
The target table cannot be an external table.
The target table cannot be a list bucketing table
*/

CREATE TRABLE dmpbean_uservipaccount
      ROW FORMAT SERED "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
      STORED AS RCFile
      AS
SELECT userid vipid,userpasswordmd5 vippassword,useremail vipemail,
FROM dmpbean_useraccount


/*

Pages  LanguageManual  LanguageManual Select
Skip to end of banner
Go to start of banner
Common Table Expression
Skip to end of metadata
Created by Harish Butani, last modified by Lars Francke on Sep 02, 2014 Go to start of metadata
A Common Table Expression (CTE) is a temporary result set derived from a simple query specified in a WITH clause,
which immediately precedes a SELECT or INSERT keyword.  The CTE is defined only within the execution scope of a single statement.
One or more CTEs can be used in a Hive SELECT, INSERT, CREATE TABLE AS SELECT, or CREATE VIEW AS SELECT statement.
Common Table Expression Syntax
withClause: cteClause (, cteClause)*
cteClause: cte_name AS (select statment)

*/

--EXAMPLE CTE in SELECT Statements
WITH q1 AS (SELECT userid FROM dmpbean_user WHERE userid ='5')
SELECT *
FROM q1

-- from style
WITH q1 AS (SELECT * FROM dmpbean_user WHERE userid='5')
FROM q1
SELECT
*;

--chain CTES
WITH q1 AS (SELECT  userid from q2 WHERE userid='5'),
q2 AS (SELECT userid from dmpbean_user WHERE userid='5')
SELECT *FROM (SELECT  userid FROM q1) a;


-- union example
WITH q1 AS (SELECT * FROM dmpbean_user WHERE userid= '5'),
q2 AS (SELECT * FROM dmpbean_user s2 WHERE userid = '4')
SELECT * FROM q1 union all SELECT * FROM q2;



--CTE in Views, CTAS, and Insert Statements
-- insert example
CREATE TABLE s1 like dmpbean_user
WITH  q1 as (SELECT userid,username FROM dmpbean_user WHERE userid=5)
FROM q1
INSERT OVERWRITE TABLE s1
SELECT *;

-- ctas example
create table s2 as
with q1 as ( select key from src where key = '4')
select * from q1;
 
-- view example
CREATE VIEW v1 AS
WITH q1 AS ( SELECT userid FROM dmpbean_user WHERE userid = '5')
SELECT * FROM q1;
SELECT * FROM v1;
 
-- VIEW example, name collision
CREATE VIEW v1 AS
WITH q1 AS ( SELECT userid FROM dmpbean_user WHERE userid = '5')
SELECT * FROM q1;
WITH q1 AS ( SELECT userid FROM dmpbean_user WHERE userid = '4')
SELECT * FROM v1;


--复制表结构
CREATE TABLE dmpbean_vipuser LIKE  dmpbean_user;


--DROP TABLE
DROP TABLE IF EXISTS dmpbean_user [PURGE]

--TRUNCATE TABLE

TRUNCATE TABLE dmpbean_user [PARTITION partition_spec]