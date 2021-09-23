from config import get_config
import csv, time, psycopg2, pyodbc
import pandas as pd
from mysql.connector import connection
from datetime import date

GENERATED_QUERIES = "test_qrys.csv"

def mysql_performance(qry_file, mysql_credentials, results_dict):

    # Create connection with MySQL DB.
    conn = connection.MySQLConnection(user=mysql_credentials.user,
                                password=mysql_credentials.password,
                                host=mysql_credentials.host,
                                database=mysql_credentials.dbname)

    # Create and open MySQL Cursor connection and csv file containing the queries.
    with conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_reader = csv.reader(csv_file, delimiter=",")

        # Starting timer, do not know yet which to use.
        # https://www.pythonpool.com/python-timer/
        # start1 = time.perf_counter()
        start2 = time.process_time()

        # Execution of the benchmark
        for qry in csv_reader:
            cur.execute(qry[0])

            for i in cur.fetchall():
                len(i)
                # print(len(i))

        # end1 = time.perf_counter()
        end2 = time.process_time()


    # elapsed_perf = end1 - start1
    elapsed_proc = end2 - start2

    # Formatting.
    # print(time.strftime("%M:%S", time.localtime(elapsed_proc)))
    # print(date.strftime("%M:%S.%f", time.localtime(elapsed_proc)))

    # results_dict["MySQL Performance"] = [elapsed_perf]
    results_dict["MySQL Result (sec)"] = [elapsed_proc]


def psql_performance(qry_file, postgres_credentials, results_dict):

    # Create connection with PostgreSQL DB.
    conn = psycopg2.connect(user=postgres_credentials.user,
                                password=postgres_credentials.password,
                                host=postgres_credentials.host,
                                database=postgres_credentials.dbname)

    # Create and open Cursor connection and csv file containing the queries.
    with conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_reader = csv.reader(csv_file, delimiter=",")

        # Starting timer, do not know yet which to use.
        # https://www.pythonpool.com/python-timer/
        start = time.process_time()

        # Execution of the benchmark
        for qry in csv_reader:
            cur.execute(qry[0])

            for i in cur.fetchall():
                len(i)
                # print(i)

        end = time.process_time()


    elapsed_time = end - start

    # Formatting.
    # print(time.strftime("%M:%S", time.localtime(elapsed_proc)))
    # print(date.strftime("%M:%S.%f", time.localtime(elapsed_proc)))

    results_dict["PostgreSQL Result (sec)"] = [elapsed_time]


def mssql_performance(qry_file, mssqlserver_credentials, results_dict):

    # Create connection with MS SQL Server DB.
    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                                user=mssqlserver_credentials.user,
                                password=mssqlserver_credentials.password,
                                host=mssqlserver_credentials.host,
                                database=mssqlserver_credentials.dbname)

    # Create and open MS SQL Server Cursor connection and csv file containing the queries.
    with conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_reader = csv.reader(csv_file, delimiter=",")

        # Starting timer, do not know yet which to use.
        # https://www.pythonpool.com/python-timer/
        start = time.process_time()

        # Execution of the benchmark
        for qry in csv_reader:
            cur.execute(qry[0])

            for i in cur.fetchall():
                len(i)
                # print(i)

        end = time.process_time()


    elapsed_time = end - start

    # Formatting.
    # print(time.strftime("%M:%S", time.localtime(elapsed_proc)))
    # print(date.strftime("%M:%S.%f", time.localtime(elapsed_proc)))

    results_dict["Microsoft SQL Server Result (sec)"] = [elapsed_time]





# Execute three different functions which execute the queries the return of all three functions is the elapsed time of execution.
def write_results_csv(dir_config, mssql, mysql, postgres, output_name):
    # Empty dictionary to store the execution time of each database.
    execution_results = {}

    qrys_csv = (dir_config.root / GENERATED_QUERIES).as_posix()

    mysql_performance(qrys_csv, mysql, execution_results)
    psql_performance(qrys_csv, postgres, execution_results)
    mssql_performance(qrys_csv, mssql, execution_results)
    # print(execution_results)

    # Creating a new DataFrame based on the dictionary 'execution_results'.
    # Saving the DataFrame as a csv file.
    results_df = pd.DataFrame(execution_results)
    print(results_df)

    results_df.to_csv((dir_config.root / output_name).as_posix() + ".csv", header=True, index=False)






def main(config):
    dir_structure = config.path
    mysql = config.mysql
    mssql = config.mssql
    psql = config.postgres

    write_results_csv(dir_structure, mssql, mysql, psql, "benchmark_generated_queries")




if __name__=='__main__':
    config = get_config("txe")
    main(config=config)