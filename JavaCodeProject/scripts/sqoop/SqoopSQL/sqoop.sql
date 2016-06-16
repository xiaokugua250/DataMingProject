create database userdb;
-- Drop table emp if exists
DROP TABLE emp;

-- Drop table emp_add if exists
DROP TABLE emp_add

-- Drop table emp_contact if exists
DROP TABLE emp_contact;



-- Creates table emp
CREATE TABLE emp (
id int,
nameString,
deg String,
salary Double,
dept String
);

-- Creates table BSEFMCG
CREATE TABLE emp_add (
id int,
hno,String,
street string,
city String
);

-- Creates table empH to contain manipulated exported data
CREATE TABLE emp_contact (
id int,
phon varchar,
email string
);

-- Creates table BSEFMCGH to contain manipulated exported data



<!-------------导入demo--------------------------------->
导入语法
$ sqoop import (generic-args) (import-args) 
$ sqoop-import (generic-args) (import-args)

下面的命令用于从MySQL数据库服务器中的emp表导入HDFS。
$ sqoop import \
--connect jdbc:mysql://localhost/userdb \
--username root \
--table emp --m 1

---------------------------------------------------------
在导入表数据到HDFS使用Sqoop导入工具，我们可以指定目标目录。

以下是指定目标目录选项的Sqoop导入命令的语法。

--target-dir <new or exist directory in HDFS>
下面的命令是用来导入emp_add表数据到'/queryresult'目录。

$ sqoop import \
--connect jdbc:mysql://localhost/userdb \
--username root \
--table emp_add \
--m 1 \     <!--the query can be executed once and imported serially, by specifying a single map task with -m 1:->
--target-dir /queryresult

"where"子句的一个子集。它执行在各自的数据库服务器相应的SQL查询，并将结果存储在HDFS的目标目录。
下面的命令用来导入emp_add表数据的子集。子集查询检索员工ID和地址，居住城市为：Secunderabad


$ sqoop import \
--connect jdbc:mysql://localhost/userdb \
--username root \
--table emp_add \
--m 1 \
--where “city =’sec-bad’” \
--target-dir /wherequery

------------------------------------------------
增量导入
增量导入是仅导入新添加的表中的行的技术。它需要添加‘incremental’, ‘check-column’, 和 ‘last-value’选项来执行增量导入。

下面的语法用于Sqoop导入命令增量选项。

--incremental <mode>
--check-column <column name>
--last value <last check column value>
让我们假设新添加的数据转换成emp表如下：

1206, satish p, grp des, 20000, GR

$ sqoop import \
--connect jdbc:mysql://localhost/userdb \
--username root \
--table emp \
--m 1 \
--incremental append \
--check-column id \
-last value 1205

----------------------------------------------------------------------------
以下语法用于导入所有表。

$ sqoop import-all-tables (generic-args) (import-args)
$ sqoop-import-all-tables (generic-args) (import-args)

$ sqoop import \
--connect jdbc:mysql://localhost/userdb \
--username root

-------------------------------------------------
以下是export命令语法。

$ sqoop export (generic-args) (export-args)
$ sqoop-export (generic-args) (export-args)

下面的命令是用来导出表数据（这是在HDFS emp_data文件）到MySQL数据库服务器DB数据库的employee表中。
$ sqoop export \
--connect jdbc:mysql://localhost/db \
--username root \
--table employee \
--export-dir /emp/emp_data
-----------------------------------------------------------------------------

 Sqoop作业创建并保存导入和导出命令。它指定参数来识别并调用已保存的工作。这种重新调用或重新执行用在增量导入，可以从RDBMS表到HDFS导入更新的行。

语法
以下是创建Sqoop作业的语法。

$ sqoop job (generic-args) (job-args)
   [-- [subtool-name] (subtool-args)]

$ sqoop-job (generic-args) (job-args)
   [-- [subtool-name] (subtool-args)

创建作业(--create)
在这里，我们创建一个名为myjob，这可以从RDBMS表的数据导入到HDFS作业。
下面的命令用于创建一个从DB数据库的employee表导入到HDFS文件的作业。

$ sqoop job --create myjob \
--import \
--connect jdbc:mysql://localhost/db \
--username root \
--table employee --m 1

验证作业 (--list)
‘--list’ 参数是用来验证保存的作业。下面的命令用来验证保存Sqoop作业的列表。

$ sqoop job --list

检查作业(--show)
‘--show’ 参数用于检查或验证特定的工作，及其详细信息。以下命令和样本输出用来验证一个名为myjob的作业。

$ sqoop job --show myjob

执行作业 (--exec)
‘--exec’ 选项用于执行保存的作业。下面的命令用于执行保存的作业称为myjob。

$ sqoop job --exec myjob

--------------------------------------------------------------------------
从面向对象应用程序的观点来看，每一个数据库表具有包含“setter”和“getter”的方法来初始化DAO类对象。
此工具(-codegen)自动生成DAO类。

它产生的DAO类在Java中是基于表的模式结构。在Java定义实例作为导入过程的一部分。
这个工具的主要用途是检查是否遗漏了Java代码。如果是这样，这将创建Java字段之间的缺省定界符的新版本。

语法
以下是Sqoop代码生成命令的语法。

$ sqoop codegen (generic-args) (codegen-args)
$ sqoop-codegen (generic-args) (codegen-args)

让我们以USERDB数据库中的表emp来生成Java代码为例。

下面的命令用来执行该给定的例子。

$ sqoop codegen \
--connect jdbc:mysql://localhost/userdb \
--username root \
--table emp

---------------------------------------------------------------------------
Sqoop “eval”工具。它允许用户执行用户定义的查询，对各自的数据库服务器和预览结果在控制台中。
这样，用户可以期望得到的表数据来导入。
使用eval我们可以评估任何类型的SQL查询可以是DDL或DML语句

以下语法用于Sqoop eval命令。

$ sqoop eval (generic-args) (eval-args)
$ sqoop-eval (generic-args) (eval-args)

选择查询评估计算
使用eval工具，我们可以评估计算任何类型的SQL查询。让我们在选择DB数据库的employee表行限制的一个例子。下面的命令用来评估计算使用SQL查询给定的例子。

$ sqoop eval \
--connect jdbc:mysql://localhost/db \
--username root \
--query “SELECT * FROM employee LIMIT 3”

Sqoop的eval工具可以适用于两个模拟和定义的SQL语句。这意味着，我们可以使用eval的INSERT语句了。下面的命令用于在DB数据库的员工(employee) 表中插入新行。

$ sqoop eval \
--connect jdbc:mysql://localhost/db \
--username root \
-e “INSERT INTO employee VALUES(1207,‘Raju’,‘UI dev’,15000,‘TP’)”

------------------------------------------------------------------------------------------
语法
以下语法用于Sqoop列表数据库命令。

$ sqoop list-databases (generic-args) (list-databases-args)
$ sqoop-list-databases (generic-args) (list-databases-args)

下面的命令用于列出MySQL数据库服务器的所有数据库。

$ sqoop list-databases \
--connect jdbc:mysql://localhost/ \
--username root

--------------------------------------------------------------------
 Sqoop的list-tables工具解析并执行针对特定数据库的“SHOW TABLES”查询。此后，它列出了在数据库中存在的表。

语法
以下是使用 Sqoop 的 list-tables  命令的语法。

$ sqoop list-tables (generic-args) (list-tables-args)
$ sqoop-list-tables (generic-args) (list-tables-args)
示例查询
下面的命令用于列出MySQL数据库服务器的USERDB数据库下的所有的表。

$ sqoop list-tables \
--connect jdbc:mysql://localhost/userdb \
--username root