from config import get_config
from database_tasks import db_performance, write_results_csv
from datetime import date, datetime
import time


def execute_benchmark(queries, db_name, db_credentials, output_name, output_dir):
    results_list = []

    # Executing queries on tables in the database.
    for i in range(1):
    # for i in range(15):
        results_list.append(db_performance(queries, db_name, db_credentials))
    
    write_results_csv({f"{db_credentials.name} Performance (seconds)": results_list}, f"{output_name}@{datetime.now().strftime('%m-%d_%H.%M')}", output_dir)

    print(f"finished with {db_credentials.name} at {datetime.now().strftime('%d-%m@%H.%M.%S')}")




def execute_original_benchmarks(original_qrys, original_db_name, db_credentials, output_dir):
    mysql = db_credentials[0]
    mssql = db_credentials[1]
    psql = db_credentials[2]

    # Executing MySQL original tables and data.
    execute_benchmark(original_qrys[0], original_db_name, mysql, "original_mysql", output_dir)
    
    # time.sleep(300)
    time.sleep(60)

    # Executing MS SQL Server original tables and data.
    execute_benchmark(original_qrys[1], original_db_name, mssql, "original_mssql", output_dir)
    

    # time.sleep(300)
    time.sleep(60)

    # Executing PostgreSQL original tables and data.
    execute_benchmark(original_qrys[2], original_db_name, psql, "original_psql", output_dir)




def execute_synthetic_benchmarks(synthetic_qrys, synthetic_db_name, db_credentials, output_dir):
    mysql = db_credentials[0]
    mssql = db_credentials[1]
    psql = db_credentials[2]

    # Executing MySQL synthetic tables and data.
    execute_benchmark(synthetic_qrys, synthetic_db_name, mysql, "synthetic_mysql", output_dir)
    
    # time.sleep(300)
    time.sleep(60)

    # Executing MS SQL Server synthetic tables and data.
    execute_benchmark(synthetic_qrys, synthetic_db_name, mssql, "synthetic_mssql", output_dir)
    
    # time.sleep(300)
    time.sleep(60)

    # Executing PostgreSQL synthetic tables and data.
    execute_benchmark(synthetic_qrys, synthetic_db_name, psql, "synthetic_psql", output_dir)




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

    execute_original_benchmarks(original_qrys_list, db_original, db_info_list, output)

    # 15 min sleep
    # time.sleep(900)

    execute_synthetic_benchmarks(qrys_generated, db_synthetic, db_info_list, output)

    
    

if __name__=='__main__':
    config = get_config("txe")
    main(config=config)