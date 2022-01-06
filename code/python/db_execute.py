from config import get_config
from database_tasks import db_performance, write_results_csv
from datetime import date, datetime


def execute_original_benchmark(original_qrys, db_name, db_credentials, output_dir):
    mysql = db_credentials[0]
    mssql = db_credentials[1]
    psql = db_credentials[2]

    mysql_original_list = []
    mssql_original_list = []
    postgresql_original_list = []

    # Executing MySQL original tables and data.
    for i in range(1):
    # for i in range(15):
        mysql_original_list.append(db_performance(original_qrys[0], db_name, mysql))
    
    write_results_csv({"MySQL Performance (seconds)": mysql_original_list}, f"original_mysql@{datetime.now().strftime('%m-%d_%H.%M')}", output_dir)

    print(f"finished with MySQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)
    time.sleep(60)

    # Executing MS SQL Server original tables and data.
    for i in range(1):
    # for i in range(15):
        mssql_original_list.append(db_performance(original_qrys[1], db_name, mssql))
    
    write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_original_list}, f"original_mssql@{datetime.now().strftime('%m-%d_%H.%M')}", output_dir)

    print(f"finished with MS SQL Server at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)
    time.sleep(60)

    # Executing PostgreSQL original tables and data.
    for i in range(1):
    # for i in range(15):
         postgresql_original_list.append(db_performance(original_qrys[2], db_name, psql))
    
    write_results_csv({"PostgreSQL Performance (seconds)": postgresql_original_list}, f"original_psql@{datetime.now().strftime('%m-%d_%H.%M')}", output_dir)

    print(f"finished with PostgreSQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")




def execute_synthetic_benchmark(synthetic_qrys, synthetic_db_name, db_credentials, output_dir):
    mysql = db_credentials[0]
    mssql = db_credentials[1]
    psql = db_credentials[2]

    mysql_synthetic_list = []
    mssql_synthetic_list = []
    postgresql_synthetic_list = []

    # Executing MySQL synthetic tables and data.
    for i in range(1):
    # for i in range(15):
        mysql_synthetic_list.append(db_performance(synthetic_qrys, synthetic_db_name, mysql))
    
    write_results_csv({"MySQL Performance (seconds)": mysql_synthetic_list}, f"synthetic_mysql@{datetime.now().strftime('%m-%d_%H.%M')}", output_dir)

    print(f"finished with MySQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    
    # time.sleep(300)
    time.sleep(60)

    # Executing MS SQL Server synthetic tables and data.
    for i in range(1):
    # for i in range(15):
        mssql_synthetic_list.append(db_performance(synthetic_qrys, synthetic_db_name, mssql))
    
    write_results_csv({"Microsoft SQL Server Performance (seconds)": mssql_synthetic_list}, f"synthetic_mssql@{datetime.now().strftime('%m-%d_%H.%M')}", output_dir)

    print(f"finished with MS SQL Server at {datetime.now().strftime('%d-%m@%H.%M.%S')}")
    

    # time.sleep(300)
    time.sleep(60)

    # Executing PostgreSQL synthetic tables and data.
    for i in range(1):
    # for i in range(15):
        postgresql_synthetic_list.append(db_performance(synthetic_qrys, synthetic_db_name, psql))

    write_results_csv({"PostgreSQL Performance (seconds)": postgresql_synthetic_list}, f"synthetic_psql@{datetime.now().strftime('%m-%d_%H.%M')}", output_dir)
    
    print(f"finished with PostgreSQL at {datetime.now().strftime('%d-%m@%H.%M.%S')}")




def main(config):
    # Setting directory paths.
    dir_logs = config.path.root / config.path.logs
    output = config.path.root / config.path.benchmarks

    # Defining paths to the different query files.
    qyrs_mysql = (dir_logs / "only_queries_mysql.csv").as_posix()
    qyrs_mssql = (dir_logs / "only_queries_mssql.csv").as_posix()
    qyrs_psql = (dir_logs / "only_queries_postgresql.csv").as_posix()
    qrys_generated = (config.path.root / "output_qcs_queries.csv").as_posix()

    original_qrys_list = [qyrs_mysql,
        qyrs_mssql,
        qyrs_psql]
    
    # Setting name original and synthetic database.
    db_original = "db_benchmark2"
    db_synthetic = "skyserver_generated"

    # Database credentials.
    db_info_list = [config.mysql,
        config.mssql,
        config.postgres]
    
    # execute_original_benchmark(original_qrys_list, db_original, db_info_list, output)

    # 15 min sleep
    # time.sleep(900)

    # execute_synthetic_benchmark(qrys_generated, db_synthetic, db_info_list, output)



    
    


if __name__=='__main__':
    config = get_config("txe")
    main(config=config)