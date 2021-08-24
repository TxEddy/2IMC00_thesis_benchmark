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


def add_data_to_table(table, column_names, dir_tables):
    # print("Directory:", dir_tables)
    # print("Table name:", table)
    # print("Column name:", table + "_" + column_name)

    table_csv = (dir_tables / table).as_posix() + ".csv"
    corr_qcs_table = (dir_tables / "correlatedAttributesQcs.csv").as_posix()
    # column = table + "_" + column_name
    # print(corr_qcs_table, type(corr_qcs_table))
    # print(table_csv, type(table_csv))

    # Create empty List and Dictionary which will be filled to rename the column names of df_corr_columns such that the columns
    # of both df's have the same name and the values of df_corr_columns can be added to df_table.
    columns = []
    rename_dict = {}
    for i in column_names:
        name = table + "_" + i.lower()
        columns.append(name)
        rename_dict[name] = i
    
    # print(columns)
    # print(rename_dict)

    # Importing complete table and afterwards select the specific columns.
    df_table = pd.read_csv(table_csv)
    df_original_columns = df_table.filter(column_names, axis=1)
    # print(df_table)
    # print(df_original_columns)
    

    # Only when importing the specific columns.
    # df_original_columns = pd.read_csv(table_csv, usecols=column_names)
    # print(df_original_columns)
    
    # Import the specific columns from the table containing the correlated attributes and renaming the column names.
    df_corr_columns = pd.read_csv(corr_qcs_table, usecols=columns).rename(columns=rename_dict)
    # print(df_corr_columns)

    # Concatenate or Append the two dataframes.
    # Dropping duplicate row, however make sure only to keep the last observation since that will be the correlated data row
    # and resetting the index.
    # While resetting the index, use the drop parameter such that the old index will not be added as a column.
    df_new = pd.concat([df_original_columns, df_corr_columns], axis=0).drop_duplicates(keep='last').reset_index(drop=True)
    # df_new = df_corr_columns.append(df_original_columns).drop_duplicates(keep='last').reset_index(drop=True)
    df_new = df_new[:len(df_table.index)]
    # print(df_new)
    
    # df_original_columns.update(df_new, overwrite=True)
    df_table.update(df_new, overwrite=True)

    # Check wether the columns are replaced.
    df_original_columns = df_table.filter(column_names, axis=1)
    print("Are the df's the same:", df_original_columns.equals(df_new))

    # Save the df containing correlated data.
    # df_table.save((dir_tables / table).as_posix() + "_new.csv", header=True, index=False)
    df_table.to_csv((dir_tables / table).as_posix() + "_new.csv", float_format="%g", header=True, index=False)




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

    # tables_qcs = output_tables / "qcs_test"
    # log_oct = output_logs / "log_oct.csv"
    # qcs_spark(tables_qcs, log_oct.as_posix())

    ####################################################
    # Creating and calculating the correlation matrix. #
    ####################################################

    # print("Output:", config.path.root)
    correlation_matrix(correlation_dir)

    #############################################################################
    # Slicing the correlation table and adding these rows to the single tables  #
    # and removing duplicates rows of the table.                                #
    #############################################################################

    # add_data_to_table("galaxy",
    #     ["clean",
    #     "dec",
    #     "g",
    #     "petroMag_r",
    #     "petroMag_u",
    #     "petroR90_g",
    #     "petroR90_r",
    #     "petroRad_u",
    #     "r",
    #     "ra"],
    #     output_tables)

    # add_data_to_table("galaxytag",
    #     ["dec",
    #     "ra",
    #     "type"],
    #     output_tables)

    # add_data_to_table("galspecextra",
    #     ["bptclass",
    #     "sfr_fib_p50",
    #     "sfr_tot_p50",
    #     "sfr_tot_p84",
    #     "specsfr_tot_p50"],
    #     output_tables)

    # add_data_to_table("galspecindx",
    #     ["d4000_n"],
    #     output_tables)

    # add_data_to_table("galspecline",
    #     ["h_alpha_eqw", "h_alpha_flux",
    #     "h_alpha_flux_err",
    #     "h_beta_eqw",
    #     "h_beta_flux",
    #     "h_beta_flux_err",
    #     "nii_6584_flux",
    #     "oi_6300_flux_err",
    #     "oiii_5007_eqw",
    #     "oiii_5007_flux",
    #     "sii_6717_flux",
    #     "sii_6731_flux_err"],
    #     output_tables)

    # add_data_to_table("photoobj",
    #     ["b",
    #     "camcol",
    #     "clean",
    #     "cModelMag_g",
    #     "dec",
    #     "deVRad_g",
    #     "deVRad_r",
    #     "fiberMag_r",
    #     "field",
    #     "flags",
    #     "fracDeV_r",
    #     "g",
    #     "l",
    #     "mode",
    #     "petroMag_r",
    #     "petroMag_z",
    #     "petroR50_g",
    #     "petroR50_r",
    #     "petroRad_g",
    #     "petroRad_r",
    #     "r",
    #     "ra",
    #     "run",
    #     "type",
    #     "u"],
    #     output_tables)

    # add_data_to_table("photoobjall",
    #     ["camcol",
    #     "clean",
    #     "dec",
    #     "dered_r",
    #     "deVRad_r",
    #     "deVRadErr_r",
    #     "expRad_r",
    #     "field",
    #     "fracDeV_r",
    #     "mode",
    #     "petroMag_r",
    #     "ra",
    #     "run",
    #     "type",
    #     "u"],
    #     output_tables)
    
    # add_data_to_table("phototag",
    #     ["clean",
    #     "dec",
    #     "mode",
    #     "nChild",
    #     "psfMag_r",
    #     "ra",
    #     "type"],
    #     output_tables)

    # add_data_to_table("photoz",
    #     ["absMagR",
    #     "photoErrorClass",
    #     "nnCount",
    #     "nnVol",
    #     "z",
    #     "zErr"],
    #     output_tables)

    # add_data_to_table("specphoto",
    #     ["class",
    #     "dec",
    #     "mode",
    #     "modelMag_r",
    #     "petroMag_r",
    #     "petroMag_z",
    #     "ra",
    #     "type",
    #     "z",
    #     "zWarning"],
    #     output_tables)
    
    # add_data_to_table("specphotoall",
    #     ["class",
    #     "dec",
    #     "mode",
    #     "modelMag_g",
    #     "modelMag_i",
    #     "modelMag_r",
    #     "modelMag_u",
    #     "modelMag_z",
    #     "petroMag_u",
    #     "petroMag_r",
    #     "ra",
    #     "sourceType",
    #     "type",
    #     "z",
    #     "zWarning"],
    #     output_tables)

    # add_data_to_table("sppparams",
    #     ["FEHADOP",
    #     "SPECTYPESUBCLASS"],
    #     output_tables)

    # add_data_to_table("stellarmassfspsgranearlydust",
    #     ["logMass",
    #     "z"],
    #     output_tables)
    
    # add_data_to_table("zoospec",
    #     ["elliptical",
    #     "p_cs",
    #     "p_cs_debiased",
    #     "p_el",
    #     "p_el_debiased",
    #     "spiral",
    #     "uncertain"],
    #     output_tables)



    

    




if __name__=='__main__':
    config = get_config("txe")
    main(config=config)