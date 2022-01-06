# 2IMC00_thesis_benchmark
Master Thesis project in which a new database benchmark is created, this benchmark is created by tables and user queries from Sloan Digital Sky Survey <sup>[1]</sup> (SDSS) Skyserver. By using queries created by users this benchmark will be different from other benchmarks, because other benchmark do not use user queries. Query Column Sets <sup>[2]</sup> (QCS) is used to find attributes which are often used in queries. The attributes in the QCS are used to generate synthetic queries and to find correlations between the tables.

[1]: http://skyserver.sdss.org/dr16/en/home.aspx
[2]: https://people.eecs.berkeley.edu/~apanda/assets/papers/eurosys13.pdf

## Downloading table data, query logs and generating synthetic data using correlation matrix
Use the Python scripts located in the folder called [python](code/python)
* The Python script [download_logs_tables.py](code/python/download_logs_tables.py) could be used to download table data and query logs.
* The Python script [synthetic_tables.py](code/python/synthetic_tables.py) could be used to generate a correlation matrix and use this matrix to generate synthetic correlated data. This script could also be used to generate the synthetic tables, and replace specific columns with those of the generated columns using the correlation matrix.
* The Python script [database_tasks.py](code/python/database_tasks.py) contains the functions to execute a benchmark. These functions are used in the Python script [db_execute.py](code/python/db_execute.py) to execute the original and synthetic data and queries on the three different databases.

### Config file
* In order that the [config.py](code/python/config.py) file works proper open this repo in VSCode.
* Add your own credentials or use the existing one.

## Query Column Sets Implementation
Use the Scala script called `QCS.scala` located in the folder called [scala](code/scala/dbBenchmarkSkyserver/src/main/scala/).
* Project could be opened in IntelliJ.

## Creating the tables and loading data in Microsoft SQL Server, MySQL and PostgreSQL
In the folder called [sql](code/sql) the different SQL statements can be found for each database, this includes create statements and statements to load the data in each database. In the scripts called *db_name*`_loading_original_tables.sql` and *db_name*`_loading_generated_tables.sql` the directories have to be changed to the directory where the original and synthetic tables are stored. These scripts which load in the data could be called in the CLI of each database.

### Microsoft SQL Server
* Used the CLI `mssql-cli`
  * When using mssql use the flag `-T 6000000` when connecting with the database in order to prevent an timeout error. Apparently a default SQL operation should not take longer than `60000 ms`.
* The `CREATE` statements for the different tables are located in the [tables](code/sql/mssql/tables) folder.
* The loading statements for the table data are located in the [mssql](code/sql/mssql) folder and could be used via the commmands: <pre>mssql> -i <i>PATH_TO/[mssql_loading_original_tables.sql](code/sql/mssql/mssql_loading_original_tables.sql)</i></pre> <pre>mssql> -i <i>PATH_TO/[mssql_loading_generated_tables.sql](code/sql/mssql/mssql_loading_generated_tables.sql)</i></pre>

* When using Docker (i.e. for Mac) first copy the original and synthetic table data to the container.
  * See [README_MSSQL.md](code/sql/mssql/README_MSSQL.md) in the `mssql` folder for more information regarding copying original and synthetic table data to the MS SQL Server container.

### MySQL
* The `CREATE` statements for the different tables are located in the [tables](code/sql/mssql/tables) folder.
* The loading statements for the table data are located in the [mysql](code/sql/mysql) folder and could be used via the commands: <pre>mysql> source <i>PATH_TO/[mysql_loading_original_tables.sql](code/sql/mysql/mysql_loading_original_tables.sql)</i></pre> <pre>mysql> source <i>PATH_TO/[mysql_loading_generated_tables.sql](code/sql/mysql/mysql_loading_generated_tables.sql)</i></pre>

### PostgreSQL
* The `CREATE` statements for the different tables are located in the [tables](code/sql/mssql/tables) folder.
* The loading statements for the table data are located in the [postgresql](code/sql/postgresql) folder and could be used via the commands: <pre>psql> \i <i>PATH_TO/[psql_loading_original_tables.sql](code/sql/postgresql/psql_loading_original_tables.sql)</i></pre> <pre>psql> \i <i>PATH_TO/[psql_loading_generated_tables.sql](code/sql/postgresql/psql_loading_generated_tables.sql)</i></pre>


## Thesis Report
The report related to this thesis project, written in LaTex, could be found in this [repository](https://github.com/TxEddy/2IMC00_thesis_report).
