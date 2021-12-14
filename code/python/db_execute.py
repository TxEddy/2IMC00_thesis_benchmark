from config import get_config
import csv, time, psycopg2, pyodbc
import pandas as pd
from mysql.connector import connection
from datetime import date, datetime

# ORIGNIAL_QUERIES_MYSQL = "only_queries_mysql.csv"
# ORIGNIAL_QUERIES_MSSQL = "only_queries_mssql.csv"
# ORIGNIAL_QUERIES_PSQL = "only_queries_postgresql.csv"
# GENERATED_QUERIES = "output_qcs_queries_new.csv"


# def mysql_performance(qry_file, mysql_credentials, dbname, results_dict):

#     # Create connection with MySQL DB.
#     conn = connection.MySQLConnection(user=mysql_credentials.user,
#                                 password=mysql_credentials.password,
#                                 host=mysql_credentials.host,
#                                 database=dbname)

#     # Create and open MySQL Cursor connection and csv file containing the queries.
#     with conn.cursor() as cur, open(qry_file) as csv_file:
#         # Read the csv file.
#         csv_reader = csv.reader(csv_file, delimiter=",")

#         # Starting timer, do not know yet which to use.
#         # https://www.pythonpool.com/python-timer/
#         start1 = time.perf_counter()

#         # Execution of the benchmark
#         for qry in csv_reader:
#             # print(qry)
#             cur.execute(qry[0])

#             for i in cur.fetchall():
#                 len(i)
#                 # print(i)

#         end1 = time.perf_counter()


#     elapsed_time = end1 - start1

#     # Formatting.
#     # print(time.strftime("%M:%S", time.localtime(elapsed_time)))

#     # results_dict["MySQL Performance (sec)"] = [elapsed_time]
#     results_dict["MySQL Performance (mm:ss)"] = [time.strftime("%M:%S", time.localtime(elapsed_time))]


# def psql_performance(qry_file, postgres_credentials, dbname, results_dict):

#     # Create connection with PostgreSQL DB.
#     conn = psycopg2.connect(user=postgres_credentials.user,
#                                 password=postgres_credentials.password,
#                                 host=postgres_credentials.host,
#                                 database=dbname)

#     # Create and open Cursor connection and csv file containing the queries.
#     with conn.cursor() as cur, open(qry_file) as csv_file:
#         # Read the csv file.
#         csv_reader = csv.reader(csv_file, delimiter=",")

#         # Starting timer, do not know yet which to use.
#         # https://www.pythonpool.com/python-timer/
#         start = time.perf_counter()

#         # Execution of the benchmark
#         for qry in csv_reader:
#             print(qry)
#             cur.execute(qry[0])

#             for i in cur.fetchall():
#                 len(i)
#                 # print(i)

#         end = time.perf_counter()


#     elapsed_time = end - start

#     # Formatting.
#     # print(time.strftime("%M:%S", time.localtime(elapsed_time)))

#     results_dict["PostgreSQL Result (mm:ss)"] = [time.strftime("%M:%S", time.localtime(elapsed_time))]


# def mssql_performance(qry_file, mssqlserver_credentials, dbname, results_dict):

#     # Create connection with MS SQL Server DB.
#     conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
#                                 user=mssqlserver_credentials.user,
#                                 password=mssqlserver_credentials.password,
#                                 host=mssqlserver_credentials.host,
#                                 database=dbname)

#     # Create and open MS SQL Server Cursor connection and csv file containing the queries.
#     with conn.cursor() as cur, open(qry_file) as csv_file:
#         # Read the csv file.
#         csv_reader = csv.reader(csv_file, delimiter=",")

#         # Starting timer, do not know yet which to use.
#         # https://www.pythonpool.com/python-timer/
#         start = time.perf_counter()

#         # Execution of the benchmark
#         for qry in csv_reader:
#             # print(qry)
#             cur.execute(qry[0])

#             for i in cur.fetchall():
#                 len(i)
#                 # print(i)

#         end = time.perf_counter()


#     elapsed_time = end - start

#     # Formatting.
#     # print(time.strftime("%M:%S", time.localtime(elapsed_time)))

#     # results_dict["Microsoft SQL Server Result (sec)"] = [elapsed_time]
#     results_dict["Microsoft SQL Server Result (mm:ss)"] = [time.strftime("%M:%S", time.localtime(elapsed_time))]


def get_db_connection(db_config, db_name):

    if (db_config.name.lower() == "mysql"):
        conn = connection.MySQLConnection(user=db_config.user,
                                password=db_config.password,
                                host=db_config.host,
                                database=db_name)
        
        header = "MySQL Execution Time (seconds)"
    
    elif (db_config.name.lower() == "microsoft sql server"):
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',
                                user=db_config.user,
                                password=db_config.password,
                                host=db_config.host,
                                database=db_name)
        
        header = "Microsoft SQL Server Execution Time (seconds)"
    
    elif (db_config.name.lower() == "postgresql"):
        conn = psycopg2.connect(user=db_config.user,
                                password=db_config.password,
                                host=db_config.host,
                                database=db_name)
        
        header = "PostgreSQL Execution Time (seconds)"

    return conn, header



def db_performance(qry_file, db_name, db_config, results_dict):

    db_conn, header = get_db_connection(db_config, db_name)

    # Create and open MySQL Cursor connection and csv file containing the queries.
    with db_conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_list = list(csv.reader(csv_file, delimiter=","))

        # Warmup of database
        for i in range(0, (round(len(csv_list) / 2))):
            # print(type(csv_list[i][0]))
            cur.execute(csv_list[i][0])

            # for i in cur.fetchall():
            #     len(i)
            #     # print(i)

            result = cur.fetchmany()
            while result:
                # for i in result:
                #     # len(i)
                #     # print(type(i))
                #     # print(tuple(i) + (5, 10))
                #     # tuple(i) + (5, 10)
                #     length_cntr += len(i)
                
                result = cur.fetchmany()

    # Create and open MySQL Cursor connection and csv file containing the queries.
    with db_conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_reader = csv.reader(csv_file, delimiter=",")

        # Starting timer, do not know yet which to use.
        # https://www.pythonpool.com/python-timer/
        start1 = time.perf_counter()

        # length_cntr = 0

        # Execution of the benchmark
        for qry in csv_reader:
            # print(qry)
            cur.execute(qry[0])

            # for i in cur.fetchall():
            #     len(i)
            #     # print(i)

            result = cur.fetchmany()
            while result:
                # for i in result:
                #     # len(i)
                #     # print(type(i))
                #     # print(tuple(i) + (5, 10))
                #     # tuple(i) + (5, 10)
                #     length_cntr += len(i)
                
                result = cur.fetchmany()

        end1 = time.perf_counter()

        # print(length_cntr)


    elapsed_time = end1 - start1

    # Formatting.
    # print(time.strftime("%M:%S", time.localtime(elapsed_time)))
    
    # results_dict[header] = [time.strftime("%M:%S", time.localtime(elapsed_time))]
    return elapsed_time


def write_results_csv(dict_results, output_name, output_dir):
    # Creating a new DataFrame based on the dictionary 'dict_results'.
    # Saving the DataFrame as a csv file.
    results_df = pd.DataFrame(dict_results)

    # print(results_df)

    # Export the results.df as csv.
    results_df.to_csv((output_dir / f"{output_name}.csv").as_posix(), header=True, index=False)


def single_qry_performance(qry_file, db_name, db_config):
    results_dict = {}
    qrys_list = []
    times_list = []
    db_conn, header = get_db_connection(db_config, db_name)

    # Create and open MySQL Cursor connection and csv file containing the queries.
    with db_conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_reader = csv.reader(csv_file, delimiter=",")

        # length_cntr = 0

        # Execution of the benchmark
        for qry in csv_reader:
            # print(qry)
            qrys_list.append(qry[0])

            start1 = time.perf_counter()
            cur.execute(qry[0])

            # for i in cur.fetchall():
            #     len(i)
            #     # print(i)

            result = cur.fetchmany()

            # print("Pickle Rick")
            # print(result)

            while result:
                # print("Morty")
                # for i in result:
                #     # len(i)
                #     # print(type(i))
                #     print(i)
                #     # print(tuple(i) + (5, 10))
                #     # tuple(i) + (5, 10)
                #     # length_cntr += len(i)
                
                result = cur.fetchmany()
            
            end1 = time.perf_counter()
            
            times_list.append(end1 - start1)
    
    results_dict['Queries'] = qrys_list
    results_dict['Execution Times'] = times_list
    # results_dict[header] = times_list

    # print(results_dict)

    return results_dict


def main(config):
    dir_structure = config.path
    mysql = config.mysql
    mssql = config.mssql
    psql = config.postgres

    today = date.today().strftime("%d-%m")

    output = dir_structure.root / "output_benchmarks"
    output_single_qry = dir_structure.root / "output_benchmark_single_qry"

    qrys_original_mysql = (dir_structure.root / dir_structure.logs / "only_queries_mysql.csv").as_posix()
    qrys_original_mssql = (dir_structure.root / dir_structure.logs / "only_queries_mssql.csv").as_posix()
    qrys_original_psql = (dir_structure.root / dir_structure.logs / "only_queries_postgresql.csv").as_posix()
    qry_aggregate = (dir_structure.root / dir_structure.logs / "aggregate_1.csv").as_posix()
    qrys_generated = (dir_structure.root / "output_qcs_queries.csv").as_posix()
    # qrys_generated = (dir_structure.root / "qcs_queries_25-09.csv").as_posix()

    execution_original = {}
    execution_synthetic = {}

    db_original_tables = "db_benchmark2"
    db_synthetic_tables = "skyserver_generated"
    test_synthetic1 = "skyserver_synthetic1"
    test_synthetic2 = "skyserver_synthetic2"
    test_synthetic3 = "skyserver_synthetic3"
    test_synthetic4 = "skyserver_synthetic4"
    test_synthetic5 = "skyserver_synthetic5"
    test_synthetic6 = "skyserver_synthetic6"
    test_synthetic7 = "skyserver_synthetic7"
    test_synthetic8 = "skyserver_synthetic8"
    test_synthetic9 = "skyserver_synthetic9"
    test_synthetic10 = "skyserver_synthetic10"
    test_synthetic11 = "skyserver_synthetic11"

    #######################################################################
    #                  THE NORMAL BENCHMARK EXECUTION                     #
    #######################################################################

    mysql_original_list = []
    mssql_original_list = []
    postgresql_original_list = []

    # # Executing MySQL original tables and data.
    # # for i in range(1):
    # for i in range(15):
    #     mysql_original_list.append(db_performance(qrys_original_mysql, db_original_tables, mysql, execution_original))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_original_list}, 'original_mysql', output)

    # print(f"finished with MySQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)
    # time.sleep(60)

    # Executing MS SQL Server original tables and data.
    # for i in range(1):
    for i in range(15):
        mssql_original_list.append(db_performance(qrys_original_mssql, db_original_tables, mssql, execution_original))
    
    write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_original_list}, 'original_mssql3', output)

    print(f"finished with MS SQL Server at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    time.sleep(300)
    time.sleep(60)

    # Executing PostgreSQL original tables and data.
    # for i in range(1):
    for i in range(15):
         postgresql_original_list.append(db_performance(qrys_original_psql, db_original_tables, psql, execution_original))
    
    write_results_csv({"PostgreSQL Performance (seconds)": postgresql_original_list}, 'original_psql3', output)

    print(f"finished with PostgreSQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # 15 min sleep
    time.sleep(900)
    # # time.sleep(300)


    # Executing MS SQL Server original tables and data.
    # for i in range(1):
    for i in range(15):
        mssql_original_list.append(db_performance(qrys_original_mssql, db_original_tables, mssql, execution_original))
    
    write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_original_list}, 'original_mssql4', output)

    print(f"finished with MS SQL Server at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    time.sleep(300)
    time.sleep(60)

    # Executing PostgreSQL original tables and data.
    # for i in range(1):
    for i in range(15):
         postgresql_original_list.append(db_performance(qrys_original_psql, db_original_tables, psql, execution_original))
    
    write_results_csv({"PostgreSQL Performance (seconds)": postgresql_original_list}, 'original_psql4', output)

    print(f"finished with PostgreSQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # 15 min sleep
    time.sleep(900)

    # ####################################################################
    # #              Synthetic test - Changed Data set                   #
    # ####################################################################
    mssql_synthetic_list = []
    postgresql_synthetic_list = []

    # Executing MS SQL Server synthetic tables and data.
    for i in range(15):
        mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic1, mssql, execution_synthetic))
    
    write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run14', output)

    print(f"finished with MS SQL Server run 12 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    time.sleep(300)

    # Executing PostgreSQL synthetic tables and data.
    for i in range(15):
        postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic1, psql, execution_synthetic))

    write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run14', output)
    
    print(f"finished with PostgreSQL run 12 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")

    # ####################################################################
    # #              Synthetic test - Changed Data set run2              #
    # ####################################################################
    time.sleep(900)

    mssql_synthetic_list = []
    postgresql_synthetic_list = []

    # Executing MS SQL Server synthetic tables and data.
    for i in range(15):
        mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic2, mssql, execution_synthetic))
    
    write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run15', output)

    print(f"finished with MS SQL Server run 13 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    time.sleep(300)

    # Executing PostgreSQL synthetic tables and data.
    for i in range(15):
        postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic2, psql, execution_synthetic))

    write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run15', output)
    
    print(f"finished with PostgreSQL run 13 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    print(f"finished at {datetime.now().strftime('%d-%m@%H.%M.%S')}")

    # ##############################
    # #      Synthetic Queries     #
    # ##############################

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # Executing MySQL synthetic tables and data.
    # for i in range(1):
    # for i in range(15):
        # mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic1, mysql, execution_synthetic))
    
    # # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_median', output)

    # # print(f"finished with MySQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # # time.sleep(300)
    # # time.sleep(60)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(1):
    # # for i in range(15):
        # mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic1, mssql, execution_synthetic))
    
    # # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_median', output)

    # print(f"finished with MS SQL Server at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # # time.sleep(300)
    # # time.sleep(60)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(1):
    # # for i in range(15):
        # postgresql_synthetic_list.append(db_performance(qrys_generated,  test_synthetic1, psql, execution_synthetic))

    # # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_median', output)
    
    # print(f"finished with PostgreSQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # #################################
    # #     Synthetic test Median     #
    # #################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # # for i in range(50):
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic2, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_mean', output)

    # print(f"finished with MySQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)
    # # time.sleep(60)

    # # Executing MS SQL Server synthetic tables and data.
    # # for i in range(50):
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic2, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_mean', output)

    # print(f"finished with MS SQL Server at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)
    # # time.sleep(60)

    # # Executing PostgreSQL synthetic tables and data.
    # # for i in range(50):
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic2, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_mean', output)
    
    # print(f"finished with PostgreSQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")

    # print(f"finished at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    ####################################################################
    #              Synthetic test - Run 1 Generated Data               #
    ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic1, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run1', output)

    # print(f"finished with MySQL run 1 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(5)

    # Executing MS SQL Server synthetic tables and data.
    # for i in range(5):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic1, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run1', output)

    # print(f"finished with MS SQL Server run 1 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(10):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic11, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run1', output)
    
    # print(f"finished with PostgreSQL run 1 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # ####################################################################
    # #              Synthetic test - Run 2 Generated Data               #
    # ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic2, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run2', output)

    # print(f"finished with MySQL run 2 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic2, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run2', output)

    # print(f"finished with MS SQL Server run 2 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic2, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run2', output)
    
    # print(f"finished with PostgreSQL run 2 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # ####################################################################
    # #              Synthetic test - Run 3 Generated Data               #
    # ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic3, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run3', output)

    # print(f"finished with MySQL run 3 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic3, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run3', output)

    # print(f"finished with MS SQL Server run 3 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic3, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run3', output)
    
    # print(f"finished with PostgreSQL run 3 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # ####################################################################
    # #              Synthetic test - Run 4 Generated Data               #
    # ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic4, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run4', output)

    # print(f"finished with MySQL run 4 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic4, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run4', output)

    # print(f"finished with MS SQL Server run 4 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic4, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run4', output)
    
    # print(f"finished with PostgreSQL run 4 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # ####################################################################
    # #              Synthetic test - Run 5 Generated Data               #
    # ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic5, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run5', output)

    # print(f"finished with MySQL run 5 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic5, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run5', output)

    # print(f"finished with MS SQL Server run 5 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic5, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run5', output)
    
    # print(f"finished with PostgreSQL run 5 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # ####################################################################
    # #              Synthetic test - Run 6 Generated Data               #
    # ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic6, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run6', output)

    # print(f"finished with MySQL run 6 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic6, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run6', output)

    # print(f"finished with MS SQL Server run 6 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic6, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run6', output)
    
    # print(f"finished with PostgreSQL run 6 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # ####################################################################
    # #              Synthetic test - Run 7 Generated Data               #
    # ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic7, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run7', output)

    # print(f"finished with MySQL run 7 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic7, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run7', output)

    # print(f"finished with MS SQL Server run 7 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic7, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run7', output)
    
    # print(f"finished with PostgreSQL run 7 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # ####################################################################
    # #              Synthetic test - Run 8 Generated Data               #
    # ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic8, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run8', output)

    # print(f"finished with MySQL run 8 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic8, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run8', output)

    # print(f"finished with MS SQL Server run 8 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic8, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run8', output)
    
    # print(f"finished with PostgreSQL run 8 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # ####################################################################
    # #              Synthetic test - Run 9 Generated Data               #
    # ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic9, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run9', output)

    # print(f"finished with MySQL run 9 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic9, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run9', output)

    # print(f"finished with MS SQL Server run 9 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic9, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run9', output)
    
    # print(f"finished with PostgreSQL run 9 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    # ####################################################################
    # #              Synthetic test - Run 10 Generated Data               #
    # ####################################################################
    # time.sleep(900)

    # mysql_synthetic_list = []
    # mssql_synthetic_list = []
    # postgresql_synthetic_list = []

    # # Executing MySQL synthetic tables and data.
    # for i in range(15):
    #     mysql_synthetic_list.append(db_performance(qrys_generated, test_synthetic10, mysql, execution_synthetic))
    
    # write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, 'synthetic_mysql_run10', output)

    # print(f"finished with MySQL run 10 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)

    # # Executing MS SQL Server synthetic tables and data.
    # for i in range(15):
    #     mssql_synthetic_list.append(db_performance(qrys_generated, test_synthetic10, mssql, execution_synthetic))
    
    # write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, 'synthetic_mssql_run10', output)

    # print(f"finished with MS SQL Server run 10 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)

    # # Executing PostgreSQL synthetic tables and data.
    # for i in range(15):
    #     postgresql_synthetic_list.append(db_performance(qrys_generated, test_synthetic10, psql, execution_synthetic))

    # write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, 'synthetic_psql_run10', output)
    
    # print(f"finished with PostgreSQL run 10 at {datetime.now().strftime('%d-%m@%H.%M.%S')}")



    # print(f"finished at {datetime.now().strftime('%d-%m@%H.%M.%S')}")


    #####################################################################################
    #                  EXECUTION TIME PER QUERY BENCHMARK EXECUTION                     #
    #####################################################################################

    # original_execution_times = {}
    # synthetic_execution_times = {}

    # for i in range(10):
    #     original_execution_times = single_qry_performance(qrys_original_mysql, db_original_tables, mysql)
    #     write_results_csv(original_execution_times, f'original_mysql_{datetime.now().strftime("%d-%m@%H.%M.%S")}', output_single_qry)
    
    # time.sleep(60)

    # for i in range(10):
    #     original_execution_times = single_qry_performance(qrys_original_mssql, db_original_tables, mssql)
    #     write_results_csv(original_execution_times, f'original_mssql_{datetime.now().strftime("%d-%m@%H.%M.%S")}', output_single_qry)
    
    # time.sleep(60)

    # for i in range(10):
    #     original_execution_times = single_qry_performance(qrys_original_psql, db_original_tables, psql)
    #     write_results_csv(original_execution_times, f'original_psql_{datetime.now().strftime("%d-%m@%H.%M.%S")}', output_single_qry)
    

    # time.sleep(300)
    

    # ##############################
    # #      Synthetic Queries     #
    # ##############################

    # for i in range(10):
    #     synthetic_execution_times = single_qry_performance(qrys_generated, db_synthetic_tables, mysql)
    #     write_results_csv(synthetic_execution_times, f'synthetic_mysql_{datetime.now().strftime("%d-%m@%H.%M.%S")}', output_single_qry)
    
    # time.sleep(60)

    # for i in range(10):
    #     synthetic_execution_times = single_qry_performance(qrys_generated, db_synthetic_tables, mssql)
    #     write_results_csv(synthetic_execution_times, f'synthetic_mssql_{datetime.now().strftime("%d-%m@%H.%M.%S")}', output_single_qry)
    
    # time.sleep(60)

    # for i in range(10):
    #     synthetic_execution_times = single_qry_performance(qrys_generated, db_synthetic_tables, psql)
    #     write_results_csv(synthetic_execution_times, f'synthetic_psql_{datetime.now().strftime("%d-%m@%H.%M.%S")}', output_single_qry)



    # print(f"finished at {datetime.now().strftime('%d-%m@%H.%M.%S')}")





if __name__=='__main__':
    config = get_config("txe")
    main(config=config)