from config import get_config
import csv, time, psycopg2, pyodbc
import pandas as pd
from mysql.connector import connection
from datetime import date

ORIGNIAL_QUERIES_MYSQL = "only_queries_mysql.csv"
ORIGNIAL_QUERIES_MSSQL = "only_queries_mssql.csv"
ORIGNIAL_QUERIES_PSQL = "only_queries_postgresql.csv"
GENERATED_QUERIES = "output_qcs_queries_new.csv"


def mysql_performance(qry_file, mysql_credentials, dbname, results_dict):

    # Create connection with MySQL DB.
    conn = connection.MySQLConnection(user=mysql_credentials.user,
                                password=mysql_credentials.password,
                                host=mysql_credentials.host,
                                database=dbname)

    # Create and open MySQL Cursor connection and csv file containing the queries.
    with conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_reader = csv.reader(csv_file, delimiter=",")

        # Starting timer, do not know yet which to use.
        # https://www.pythonpool.com/python-timer/
        start1 = time.perf_counter()

        # Execution of the benchmark
        for qry in csv_reader:
            # print(qry)
            cur.execute(qry[0])

            for i in cur.fetchall():
                len(i)
                # print(i)

        end1 = time.perf_counter()


    elapsed_time = end1 - start1

    # Formatting.
    # print(time.strftime("%M:%S", time.localtime(elapsed_time)))

    # results_dict["MySQL Performance (sec)"] = [elapsed_time]
    results_dict["MySQL Performance (mm:ss)"] = [time.strftime("%M:%S", time.localtime(elapsed_time))]


def psql_performance(qry_file, postgres_credentials, dbname, results_dict):

    # Create connection with PostgreSQL DB.
    conn = psycopg2.connect(user=postgres_credentials.user,
                                password=postgres_credentials.password,
                                host=postgres_credentials.host,
                                database=dbname)

    # Create and open Cursor connection and csv file containing the queries.
    with conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_reader = csv.reader(csv_file, delimiter=",")

        # Starting timer, do not know yet which to use.
        # https://www.pythonpool.com/python-timer/
        start = time.perf_counter()

        # Execution of the benchmark
        for qry in csv_reader:
            print(qry)
            cur.execute(qry[0])

            for i in cur.fetchall():
                len(i)
                # print(i)

        end = time.perf_counter()


    elapsed_time = end - start

    # Formatting.
    # print(time.strftime("%M:%S", time.localtime(elapsed_time)))

    results_dict["PostgreSQL Result (mm:ss)"] = [time.strftime("%M:%S", time.localtime(elapsed_time))]


def mssql_performance(qry_file, mssqlserver_credentials, dbname, results_dict):

    # Create connection with MS SQL Server DB.
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                                user=mssqlserver_credentials.user,
                                password=mssqlserver_credentials.password,
                                host=mssqlserver_credentials.host,
                                database=dbname)

    # Create and open MS SQL Server Cursor connection and csv file containing the queries.
    with conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_reader = csv.reader(csv_file, delimiter=",")

        # Starting timer, do not know yet which to use.
        # https://www.pythonpool.com/python-timer/
        start = time.perf_counter()

        # Execution of the benchmark
        for qry in csv_reader:
            # print(qry)
            cur.execute(qry[0])

            for i in cur.fetchall():
                len(i)
                # print(i)

        end = time.perf_counter()


    elapsed_time = end - start

    # Formatting.
    # print(time.strftime("%M:%S", time.localtime(elapsed_time)))

    # results_dict["Microsoft SQL Server Result (sec)"] = [elapsed_time]
    results_dict["Microsoft SQL Server Result (mm:ss)"] = [time.strftime("%M:%S", time.localtime(elapsed_time))]


def get_db_connection(db_config, db_name):

    if (db_config.name.lower() == "mysql"):
        conn = connection.MySQLConnection(user=db_config.user,
                                password=db_config.password,
                                host=db_config.host,
                                database=db_name)
        
        header = "MySQL Performance (mm:ss)"
    
    elif (db_config.name.lower() == "microsoft sql server"):
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                                user=db_config.user,
                                password=db_config.password,
                                host=db_config.host,
                                database=db_name)
        
        header = "Microsoft SQL Server Performance (mm:ss)"
    
    elif (db_config.name.lower() == "postgresql"):
        conn = psycopg2.connect(user=db_config.user,
                                password=db_config.password,
                                host=db_config.host,
                                database=db_name)
        
        header = "PostgreSQL Performance (mm:ss)"

    return conn, header



def db_performance(qry_file, db_name, db_config, results_dict):

    db_conn, header = get_db_connection(db_config, db_name)

    # Create and open MySQL Cursor connection and csv file containing the queries.
    with db_conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_reader = csv.reader(csv_file, delimiter=",")

        # Starting timer, do not know yet which to use.
        # https://www.pythonpool.com/python-timer/
        start1 = time.perf_counter()

        # Execution of the benchmark
        for qry in csv_reader:
            # print(qry)
            cur.execute(qry[0])

            for i in cur.fetchall():
                len(i)
                # print(i)

        end1 = time.perf_counter()


    elapsed_time = end1 - start1

    # Formatting.
    # print(time.strftime("%M:%S", time.localtime(elapsed_time)))
    
    results_dict[header] = [time.strftime("%M:%S", time.localtime(elapsed_time))]


def write_results_csv(dict_results, output_name, output_dir):
    # Creating a new DataFrame based on the dictionary 'dict_results'.
    # Saving the DataFrame as a csv file.
    results_df = pd.DataFrame(dict_results)

    # print(results_df)

    # Export the results.df as csv.
    results_df.to_csv((output_dir / f"{output_name}.csv").as_posix(), header=True, index=False)


def main(config):
    dir_structure = config.path
    mysql = config.mysql
    mssql = config.mssql
    psql = config.postgres

    qrys_original_mysql = (dir_structure.root / dir_structure.logs / "only_queries_mysql.csv").as_posix()
    qrys_original_mssql = (dir_structure.root / dir_structure.logs / "only_queries_mssql.csv").as_posix()
    qrys_original_psql = (dir_structure.root / dir_structure.logs / "only_queries_postgresql.csv").as_posix()
    qrys_generated = (dir_structure.root / "output_qcs_queries_new.csv").as_posix()

    execution_original = {}
    execution_synthetic = {}

    db_original_tables = "db_benchmark"
    db_synthetic_tables = "skyserver_generated"

    # Executing benchmark on original tables and data.
    db_performance(qrys_original_mysql, db_original_tables, mysql, execution_original)
    db_performance(qrys_original_mssql, db_original_tables, mssql, execution_original)
    db_performance(qrys_original_psql, db_original_tables, psql, execution_original)

    # print(execution_original)

    # Converting dict to df and save as csv.
    write_results_csv(execution_original, "benchmark_original_queries", dir_structure.root)


    # Executing benchmark on synthetic tables and data.
    db_performance(qrys_generated, db_synthetic_tables, mysql, execution_synthetic)
    db_performance(qrys_generated, db_synthetic_tables, mssql, execution_synthetic)
    db_performance(qrys_generated, db_synthetic_tables, psql, execution_synthetic)

    # print(execution_synthetic)

    # Converting dict to df and save as csv.
    write_results_csv(execution_synthetic, "benchmark_synthetic_queries", dir_structure.root)





if __name__=='__main__':
    config = get_config("txe")
    main(config=config)