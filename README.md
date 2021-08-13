# 2IMC00_thesis_benchmark
Master Thesis project in which a new database benchmark is created, this benchmark is created by tables and user queries from Sloan Digital Sky Survey <sup>[1]</sup> (SDSS) Skyserver. By using queries created by users this benchmark will be different from other benchmarks, because other benchmark do not use user queries. Query Column Sets <sup>[2]</sup> (QCS) is used to find attributes which are often used in queries. The attributes in the QCS are used to generate synthetic queries and to find correlations between the tables.

[1]: http://skyserver.sdss.org/dr16/en/home.aspx
[2]: https://people.eecs.berkeley.edu/~apanda/assets/papers/eurosys13.pdf

## Downloading table data, query logs and generating data usinig correlation matrix
Use the Python scripts located in the folder called [python](code/python)
* The Python script `...` could be used to download table data and query logs.
* The Python script `...` could be used to generate a correlation matrix and use this matrix to generate synthetic correlated data.

## Query Column Sets Implementation
Use the Scala script called `QCS.scala` located in the folder called [scala](code/scala/dbBenchMarkSkyserver/src/main/scala/)

## Thesis Report
The report related to this thesis project, written in LaTex, could be found in this [repository](https://github.com/TxEddy/2IMC00_thesis_report).
