# 2IMC00_thesis_benchmark
Master Thesis project in which a new database benchmark is created, this benchmark is created by tables and user queries from Sloan Digital Sky Survey <sup>[1]</sup> (SDSS) Skyserver. By using queries created by users this benchmark will be different from other benchmarks, because other benchmark do not use user queries. Query Column Sets <sup>[2]</sup> (QCS) is used to find attributes which are often used in queries. The attributes in the QCS are used to generate synthetic queries and to find correlations between the tables.

[1]: http://skyserver.sdss.org/dr16/en/home.aspx
[2]: https://people.eecs.berkeley.edu/~apanda/assets/papers/eurosys13.pdf

## Downloading table data, query logs and generating data usinig correlation matrix
Use the Python scripts located in the folder called [python](code/python)
* The Python script [test_logs_data.py](code/python/test_logs_data.py) could be used to download table data and query logs.
* The Python script [test_process_logs.py](code/python/test_process_logs.py) could be used to generate a correlation matrix and use this matrix to generate synthetic correlated data.
* The Python script `...` could be used to execute the original and synthetic data and queries on the three different databases.

### Config file
* In order that the [config.py](code/python/config.py) file works proper open this repo in VSCode.
* Add your own credentials or use the existing one.

## Query Column Sets Implementation
Use the Scala script called `QCS.scala` located in the folder called [scala](code/scala/dbBenchMarkSkyserver/src/main/scala/)
* Project could be opened in IntelliJ

## Creating the tables and loading data in Microsoft SQL Server, MySQL and PostgreSQL
In the folder called [sql](code/sql) the different SQL statements can be found for each database, this includes create statements and statements to load the data in each database. In the scripts called `...`, `...` and `...` the directories have to be changed to the directory where the original and synthetic tables are stored. These scripts to load the data could be called in the CLI of each database.

### Microsoft SQL Server
* Used the CLI mssql
  * When using mssql use the flag `-T 6000000` when connecting with the database in order to prevent an timeout error. Apparently a default SQL operation should not take longer than `... ms`.
* The loading statements for the data located in `...` could be used via the commmand: <pre>mssql> -i <i>PATH_TO/[loading_tables_db.sql](code/sql/loading_tables_db.sql)</i></pre>
  * When using Docker (i.e. for Mac) first copy the original and synthetic data to the container using the commands:  
    `...`
  * Or see `README.md(create link to actual readme)` in mssql folder.

### MySQL
* The loading statements for the data located in `...` could be used via the command: <pre>mysql>?? <i>PATH_TO/[loading_tables_db.sql](code/sql/loading_tables_db.sql)</i></pre>

### PostgreSQL
* The loading statements for the data located in `...` could be used via the command: <pre>??> \s or \i??? <i>PATH_TO/[loading_tables_db.sql](code/sql/loading_tables_db.sql)</i></pre>


## Thesis Report
The report related to this thesis project, written in LaTex, could be found in this [repository](https://github.com/TxEddy/2IMC00_thesis_report).
