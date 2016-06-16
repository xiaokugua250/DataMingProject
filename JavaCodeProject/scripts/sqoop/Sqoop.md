### sqoop 文档

1. **Introduction**
Sqoop is a tool designed to transfer data between Hadoop and relational databases or mainframes.
You can use Sqoop to import data from a relational database management system (RDBMS) such as MySQL or Oracle or a mainframe into the Hadoop Distributed File System (HDFS),
transform the data in Hadoop MapReduce, and then export the data back into an RDBMS.
Sqoop automates most of this process, relying on the database to describe the schema for the data to be imported. Sqoop uses MapReduce to import and export the data, which provides parallel operation as well as fault tolerance.

2. **参考示例**
- [IBM导入导出示例](http://www.ibm.com/developerworks/cn/data/library/bd-sqoopconduit/)
- [sqoop中文手册](http://www.zihou.me/html/2014/01/28/9114.html)
- [sqoop定时增量导入](http://blog.csdn.net/ryantotti/article/details/14226635)
- [sqoop教程](http://www.yiibai.com/sqoop/sqoop_import.html)