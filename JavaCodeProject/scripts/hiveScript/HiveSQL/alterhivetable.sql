/*
Rename Table
ALTER TABLE table_name RENAME TO new_table_name;
This statement lets you change the name of a table to a different name.
As of version 0.6, a rename on a managed table moves its HDFS location as well. (Older Hive versions just renamed the table in the metastore without moving the HDFS location.)
Alter Table Properties
ALTER TABLE table_name SET TBLPROPERTIES table_properties;

table_properties:
  : (property_name = property_value, property_name = property_value, ... )
You can use this statement to add your own metadata to the tables. Currently last_modified_user, last_modified_time properties are automatically added and managed by Hive. Users can add their own properties to this list. You can do DESCRIBE EXTENDED TABLE to get this information.
Alter Table Comment
To change the comment of a table you have to change the comment property of the TBLPROPERTIES:
ALTER TABLE table_name SET TBLPROPERTIES ('comment' = new_comment);
Add SerDe Properties
ALTER TABLE table_name [PARTITION partition_spec] SET SERDE serde_class_name [WITH SERDEPROPERTIES serde_properties];

ALTER TABLE table_name [PARTITION partition_spec] SET SERDEPROPERTIES serde_properties;

serde_properties:
  : (property_name = property_value, property_name = property_value, ... )
*/

/*

Add Partitions
ALTER TABLE table_name ADD [IF NOT EXISTS] PARTITION partition_spec
  [LOCATION 'location1'] partition_spec [LOCATION 'location2'] ...;

partition_spec:
  : (partition_column = partition_col_value, partition_column = partition_col_value, ...)
You can use ALTER TABLE ADD PARTITION to add partitions to a table. Partition values should be quoted only if they are strings. The location must be a directory inside of which data files reside. (ADD PARTITION changes the table metadata, but does not load data. If the data does not exist in the partition's location, queries will not return any results.) An error is thrown if the partition_spec for the table already exists. You can use IF NOT EXISTS to skip the error.
Version 0.7
Although it is proper syntax to have multiple partition_spec in a single ALTER TABLE, if you do this in version 0.7 your partitioning scheme will fail. That is, every query specifying a partition will always use only the first partition.
Specifically, the following example will FAIL silently and without error in Hive 0.7, and all queries will go only to dt='2008-08-08' partition, no matter which partition you specify.
Example:
ALTER TABLE page_view ADD PARTITION (dt='2008-08-08', country='us') location '/path/to/us/part080808'
                          PARTITION (dt='2008-08-09', country='us') location '/path/to/us/part080809';
 */

