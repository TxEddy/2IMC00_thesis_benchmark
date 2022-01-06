import csv, time, psycopg2, pyodbc
import pandas as pd
from mysql.connector import connection

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



def db_performance(qry_file, db_name, db_config):

    db_conn, header = get_db_connection(db_config, db_name)

    # Create and open MySQL Cursor connection and csv file containing the queries.
    with db_conn.cursor() as cur, open(qry_file) as csv_file:
        # Read the csv file.
        csv_list = list(csv.reader(csv_file, delimiter=","))

        # Warmup of database
        for i in range(0, (round(len(csv_list) / 2))):
            # print(csv_list[i][0])
            cur.execute(csv_list[i][0])

            # for i in cur.fetchall():
            #     len(i)
            #     # print(i)

            result = cur.fetchmany()
            while result:
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
                result = cur.fetchmany()

        end1 = time.perf_counter()

        # print(length_cntr)


    elapsed_time = end1 - start1

    # Formatting.
    # print(time.strftime("%M:%S", time.localtime(elapsed_time)))

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
                result = cur.fetchmany()
            
            end1 = time.perf_counter()
            
            times_list.append(end1 - start1)
    
    results_dict['Queries'] = qrys_list
    results_dict['Execution Times'] = times_list
    # results_dict[header] = times_list

    # print(results_dict)

    return results_dict