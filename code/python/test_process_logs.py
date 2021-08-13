from config import get_config
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType
from pathlib import Path
from itertools import islice
from py4j.java_gateway import get_field
import re, csv, json
import pandas as pd
from scipy.stats import multivariate_normal as mvn

# Global dictionary variable for counting tables.
freq_table = {}

def frequency_table(file_, dir):
    spark = SparkSession.builder.getOrCreate()
    # pattern_table = r".+FROM\ (\w+).+"
    # pattern_table = r".+(FROM|from)\ (\w+).+"
    pattern_table = r".+(FROM|from)\ +\.?\.?(\w+).+"
    # pattern_dismiss = r"f.+|specobjall|SpecObjAll|specobj|SpecObj|dbo"
    pattern_dismiss = r"f.+|specobj|specobjall|SpecObj|SpecObjAll|dbo"

    # Get all csv files in the given directory.
    # files_csv = dir.glob('**/*.csv') # Also get csv files in underlying folders.
    files_csv = dir.glob('*.csv')
    paths_csv = [path.as_posix() for path in files_csv]
    
    df = spark.read.options(header=True, delimiter=",").csv(file_)
    list_queries = df.select("statement").collect()

    # Create empty Dictionary.
    # freq_table = {}

    # Using the global freq_table Dictionary.
    global freq_table

    for qry in list_queries:
        if qry.statement is not None and re.search(pattern_table, qry.statement):
            match = re.match(pattern_table, qry.statement)
            # print("Full match:", match)
            # print("Regex match is:", match.group(2))

            if bool(re.match(pattern_dismiss, match.group(2))):
                continue

            # Check if the table name, in lowercase, is already in the dictionary, otherwise create new key. Is made lowercase, since queries sometimes have the tablenames as CamelCase.
            if (match.group(2).lower() in freq_table):
                freq_table[match.group(2).lower()] += 1
            else:
                freq_table[match.group(2).lower()] = 1

    # Sort the dictionary on values in ascending order.
    freq_table = dict(sorted(freq_table.items(), key=lambda item:(item[1], item[0]), reverse=True))
    
    # Output name of the whole dictionary.
    out_dict = "freq_table_names.csv"
    # print(out_dict)

    # Output name of top 20 of freq_dict and selecting top 20 of the Dictionary freq_dict.
    out_dict_selection = "freq_table_top20.csv"
    freq_top = dict(islice(freq_table.items(), 20))
    
    # Write global Dictionary freq_table to a csv file.
    with open(out_dict, 'w') as csv_complete, open(out_dict_selection, 'w') as csv_selection:
        writer_complete = csv.writer(csv_complete)
        writer_selection = csv.writer(csv_selection)

        for key, value in freq_table.items():
            writer_complete.writerow([key, value])
        
        for key, value in freq_top.items():
            writer_selection.writerow([key, value])


def open_table(file_, dir):
    # Configure Spark with maxToStringFields, spark.driver.memory to prevent java.lang.OutOfMemoryError.
    spark = SparkSession\
        .builder\
        .config("spark.sql.debug.maxToStringFields", "1000")\
        .config("spark.driver.memory", "2g")\
        .getOrCreate()

    # Get all csv files in the given directory.
    # files_csv = dir.glob('**/*.csv') # Also get csv files in underlying folders.
    files_csv = dir.glob('*.csv')
    paths_csv = [path.as_posix() for path in files_csv]

    
    df = spark.read.options(header=True, delimiter=",").csv(file_.as_posix())
    # print(df.show())

    # Getting only file name.
    # print(file_.parts[-1].split(".")[0] + "_stats")

    # Define output directory
    output = (dir / (file_.parts[-1].split(".")[0] + "_stats")).as_posix()
    # print(type(output))
    # print(output)

    df_stats = df.describe()
    # df_stats.coalesce(1).write.csv(output, mode="append", header=True)
    df_stats.coalesce(1).write.csv(output, mode="overwrite", header=True)

    # df.printSchema()


def table_correlation(csv_table):
    # Configure Spark with maxToStringFields, spark.driver.memory to prevent java.lang.OutOfMemoryError.
    spark = SparkSession\
        .builder\
        .config("spark.sql.debug.maxToStringFields", "1000")\
        .config("spark.driver.memory", "2g")\
        .getOrCreate()

    # Loading schema from predefined json file.
    table_name = csv_table.name.split(".")[0] + ".json"
    schema_name = csv_table.parents[1] / "table_schemas" / table_name
    with open(schema_name.as_posix()) as json_file:
        schema_json = StructType.fromJson(json.load(json_file))
    
    # Loading csv using custom schema.
    df = spark.read.options(header=True, delimiter=",").schema(schema_json).csv(csv_table.as_posix())

    # df = spark.read.options(header=True, delimiter=",").csv(csv_table.as_posix())

    df.createTempView("sdssebossfirefly")
    
    spark.sql("select * from sdssebossfirefly where specobjid like '1%'").explain(mode='extended')

    # qry_results.explain()


def qcs_regex(log_file):
    spark = SparkSession.builder.getOrCreate()    

    pattern_column = r".+(FROM|from)\ +(.+)(WHERE|where)\ +(\w+\.?\w*)"
    # pattern_test = r".+(FROM|from).+(JOIN|join)?.+?(ON|on)?\ ?(.+)?(WHERE|where)\ +(\w+\.?\w*)"

    qcs_dict = {}

    df = spark.read.options(header=True, delimiter=",").csv(log_file.as_posix())
    list_queries = df.select("statement").collect()

    number_rows = df.count()

    for qry in list_queries:
        if qry.statement is not None and re.search(pattern_column, qry.statement):
            match = re.match(pattern_column, qry.statement)
            # print("Full match:", match)
            # print("Regex match is:", match.group(2))            

            # Check if the table name, in lowercase, is already in the dictionary, otherwise create new key. Is made lowercase, since queries sometimes have the tablenames as CamelCase.
            if match.group(4).lower() in qcs_dict and match.group(2).lower() in qcs_dict[match.group(4).lower()][1]:
                qcs_dict[match.group(4).lower()][0] += 1
            elif match.group(4).lower() in qcs_dict and match.group(2).lower() not in qcs_dict[match.group(4).lower()][1]:
                qcs_dict[match.group(4).lower()][0] += 1
                qcs_dict[match.group(4).lower()][1].append(match.group(2).lower())
            else:
                qcs_dict[match.group(4).lower()] = [1, [match.group(2).lower()]]


    for key, value in qcs_dict.items():
        # print(qcs_dict[key])
        percent = (qcs_dict[key][0] / number_rows) * 100
        qcs_dict[key][0] = f"{percent:.2f}%"
    

        
    # Output name of top 20 of freq_dict and selecting top 20 of the Dictionary freq_dict.
    out_simple_qcs = "qcs_test.csv"
    
    # Write global Dictionary freq_table to a csv file.
    with open(out_simple_qcs, 'w') as csv_qcs:
        writer_complete = csv.writer(csv_qcs)

        for key, value in qcs_dict.items():
            writer_complete.writerow([key, value])


def qcs_spark(table_dir, log):
    # Configure Spark with maxToStringFields, spark.driver.memory to prevent java.lang.OutOfMemoryError.
    spark = SparkSession\
        .builder\
        .config("spark.sql.debug.maxToStringFields", "1000")\
        .config("spark.driver.memory", "2g")\
        .getOrCreate()
    
    # sc = SparkContext('local', 'Spark SQL')


    # Get all csv files in the given directory.
    # files_csv = dir.glob('**/*.csv') # Also get csv files in underlying folders.
    files_csv = table_dir.glob('*.csv')
    paths_csv = [path.as_posix() for path in files_csv]

    for i in paths_csv:
        table_name = i.split("/")[-1].split(".")[0]

        df = spark.read.options(header=True, delimiter=",").csv(i)
        df.createTempView(table_name)
    
    # qry_log = spark.read.options(header=True, delimiter=",").csv(log)
    # qrys = qry_log.select("statement").collect()

    # for qry in qrys:
    #     print(qry.statement)

    # test = spark.sql("SELECT COUNT(p.objid) as CNT FROM PhotoObjAll AS p JOIN SpecObjAll s ON p.objID = s.bestObjID WHERE p.dec >= 52 and p.dec < 60")
    # test = spark.sql("SELECT COUNT(p.objid) as CNT FROM PhotoObjAll AS p JOIN SpecObjAll s ON p.objID = s.bestObjID WHERE p.dec >= 52 and p.dec < 60")._jdf.queryExecution().toString()

    # test = spark.sql("SELECT COUNT(p.objid) as CNT FROM PhotoObjAll AS p JOIN SpecObjAll s ON p.objID = s.bestObjID WHERE p.dec >= 52 and p.dec < 60")
    # print(type(test._sc._jvm.PythonSQLUtils.explainString(test._jdf.queryExecution(), 'extended')))
    # print(type(test._jdf.queryExecution()))

    # test = spark.sql("SELECT COUNT(p.objid) as CNT FROM PhotoObjAll AS p JOIN SpecObjAll s ON p.objID = s.bestObjID WHERE p.dec >= 52 and p.dec < 60")._jdf.queryExecution().analyzed().aggregate.get()
    test = spark.sql("SELECT COUNT(p.objid) as CNT FROM PhotoObjAll AS p JOIN SpecObjAll s ON p.objID = s.bestObjID WHERE p.dec >= 52 and p.dec < 60")._jdf.queryExecution().analyzed()

    # print(test_splitted)

    # print(test)
    print(type(test))


def correlation_matrix(qcs_tables_dir):
    # Directory containing data output with all attributes stated in the QCS.
    dir_qcs_attributes = qcs_tables_dir / "qcsAttributesOutput"

    # Get all csv files in the given directory, this is the same directory as the output directory in Spark to export the df containing data of all attributes states in the QCS. Since Spark exports the df with a random name, globally get the csv files in the given directory.
    # files_csv = qcs_tables_dir.glob('**/*.csv') # Also get csv files in underlying folders.
    files_csv = dir_qcs_attributes.glob('**/*.csv') # Also get csv files in underlying folders.
    paths_csv = [path.as_posix() for path in files_csv]

    # print(paths_csv)
    # print(paths_csv[0])

    # Spark exports one csv file, therefore the path of the file is on position 0.
    qry_df = pd.read_csv(paths_csv[0])
    # print(qry_df.head(5))
    # print(qry_df.dtypes)

    # Create an correlation matrix based on the attributes using the Pearson method.
    corr_matrix = qry_df.corr(method='pearson')
    # print(corr_matrix)

    # Filling the correlation matrix with 0's when there is a NaN value and export this correlation matrix as csv.
    corr_matrix = corr_matrix.fillna(0)
    # corr_matrix.to_csv(qcs_tables_dir.parents[0].as_posix() + "/testMatrixGenerate/qcsQry_complete_matrix.csv")
    corr_matrix.to_csv(qcs_tables_dir.as_posix() + "/qcs_attributes_matrix.csv")

    # Generate data using the correlation matrix and multivariate normal distribution sampling.
    # generated_corr = mvn.rvs(mean=qry_df.mean().array, cov=corr_matrix, size=15000)
    generated_corr = mvn.rvs(mean=qry_df.mean().array, cov=corr_matrix, size=150)
    # print(generated_corr)

    # Adding the column names to the generated data and export the generated data as csv.
    generated_df = pd.DataFrame(data = generated_corr, columns = qry_df.columns)
    generated_df.to_csv(qcs_tables_dir.as_posix() + "/qcs_attributes_generated.csv", index=False)


def main(config):
    output_logs = config.path.root / config.path.logs
    output_tables = config.path.root / config.path.tables
    qcs_frequency_dir = config.path.root / "correlation_tables" / "qcs_qry_output"
    correlation_dir = config.path.root / config.path.correlations

    # csv_log_oct = output_logs / "log_oct.csv"
    # frequency_table(csv_log_oct.as_posix(), output_logs)

    # csv_log_nov = output_logs / "log_nov.csv"
    # frequency_table(csv_log_nov.as_posix(), output_logs)

    # csv_log_dec = output_logs / "log_dec.csv"
    # frequency_table(csv_log_dec.as_posix(), output_logs)

    # csv_log_202 = output_logs / "log_2020.csv"
    # frequency_table(csv_log_202.as_posix(), output_logs)

    # csv_table_photo_obj_all = output_tables / "photo_obj_all.csv"
    # open_table(csv_table_photo_obj_all, output_tables)

    # table2 = output_tables / "sdss_eboss_firefly.csv"
    # table_correlation(table2)

    # log_oct = output_logs / "log_oct.csv"
    # qcs_regex(log_oct)

    # tables_qcs = output_tables / "qcs_test"
    # log_oct = output_logs / "log_oct.csv"
    # qcs_spark(tables_qcs, log_oct.as_posix())

    # correlation_matrix(qcs_frequency_dir)
    correlation_matrix(correlation_dir)
    # print("Output:", config.path.root)




if __name__=='__main__':
    config = get_config("txe")
    main(config=config)