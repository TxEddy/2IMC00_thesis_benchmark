from numpy.core.fromnumeric import size
from config import get_config
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType
from pathlib import Path
from itertools import islice
import re, csv, json, math, random
import pandas as pd
from scipy.stats import distributions, multivariate_normal as mvn
from faker import Faker
from pydbgen import pydbgen
import numpy as np
import matplotlib
matplotlib.use('PS')
from fitter import Fitter #, get_common_distributions, get_distributions


def generate_data(attribute_name, df_table, table_schema, amount_generate):
    fake = Faker()
    # print(attribute_name)

    # pd.set_option("max_columns", None)
    # table_df = pd.read_csv(table_file)
    # print(table_df)
    # print(table_df["class"].apply(len).mean().round(0))

    data_list = list()
    str_length = 0
    specific_int = ""

    for i in table_schema:
        if (attribute_name in i.values()):
            print("Name:", i["name"])
            
            if ("specific type" in i["metadata"]):
                specific_int = i["metadata"]["specific type"]
            
            for j in range(1, amount_generate + 1):
                if ("primary key" in i["metadata"]):
                    data_list.append(j)
                
                elif ("foreign key" in i["metadata"]):
                    # data_list.append(fake.unique.pyint(1, amount_generate))
                    data_list = list(range(1, amount_generate + 1))
                    random.shuffle(data_list)
                    # adding a break, since a complete list is generated here therefore unnecessary to do this multiple times.
                    break
                    
                elif (i["type"].lower() == "integer" or i["type"].lower() == "long" or i["type"].lower() == "double" or i["type"].lower() == "float"):
                    if (specific_int == "tinyint"):
                        data_list.append(fake.pyint(max_value=127))
                    
                    else:
                        data_list.append(fake.pyint())
                
                elif (i["type"].lower() == "double" or i["type"].lower() == "float"):
                    data_list.append(fake.pyfloat())
                
                elif (i["type"].lower() == "string" or i["type"].lower() == "varchar"):
                    # data_list.append(fake.name())
                    # data_list.append(fake.pystr(max_chars=math.ceil(df_table[i["name"]].fillna(method='ffill').astype(str).apply(len).mean())))
                    # data_list.append(fake.pystr(max_chars=str_length))
                    # print(f"length of {i['name']}:", table_df[i["name"]].apply(len).mean().round(0))
                    # print(i["name"], table_df[i["name"]].fillna(method='ffill').astype(str).apply(len).mean().round(0))
                    # print("Length string:", i["metadata"]["varchar"])
                    # print(fake.pystr(min_chars=1, max_chars=10))
                    data_list.append(fake.pystr(min_chars=1, max_chars=i["metadata"]["varchar"]))
    
    return data_list


def distribution_generate(distribution_parameters, number_generate):
    dist = distribution_parameters[0]

    if (dist == "beta"):
        np_array = np.random.beta(a=distribution_parameters[1], b=distribution_parameters[2], size=number_generate)

    elif(dist == "f"):
        np_array = np.random.f(dfnum=distribution_parameters[1], dfden=distribution_parameters[2], size=number_generate)

    elif(dist == "gamma"):
        np_array = np.random.gamma(shape=distribution_parameters[1], scale=distribution_parameters[3], size=number_generate)

    elif(dist == "laplace"):
        np_array = np.random.laplace(loc=distribution_parameters[1], scale=distribution_parameters[2], size=number_generate)

    elif(dist == "logistic"):
        np_array = np.random.logistic(loc=distribution_parameters[1], scale=distribution_parameters[2], size=number_generate)

    elif(dist == "normal"):
        np_array = np.random.normal(loc=distribution_parameters[1], scale=distribution_parameters[2], size=number_generate)

    elif(dist == "rayleigh"):
        np_array = np.random.rayleigh(scale=distribution_parameters[2], size=number_generate)

    elif(dist == "uniform"):
        np_array = np.random.uniform(size=number_generate)
    

    # print(np_array)
    # print(np_array.tolist())
    return np_array.tolist()
    



def generate_data2(attribute_name, df_table, table_schema, amount_generate):
    fake = Faker()
    # print(attribute_name)

    # pd.set_option("max_columns", None)
    # table_df = pd.read_csv(table_file)
    # print(table_df)
    # print(table_df["class"].apply(len).mean().round(0))

    data_list = list()
    params_list = list()
    specific_int = ""
    
    distribution_list = ["beta",
                "f",
                "gamma",
                "laplace",
                "logistic",
                "normal",
                "rayleigh",
                "uniform"]

    for i in table_schema:
        if (attribute_name in i.values()):
            print("Name:", i["name"])
            
            if ((i["type"].lower() == "integer" or i["type"].lower() == "long") and "foreign key" not in i["metadata"] and "primary key" not in i["metadata"] and i["name"] != "specobjid"):
                    # print(df_table[i["name"]])
                    # print("numbers")
                    tmp_list = list()
                    data_np = df_table[i["name"]].values

                    f = Fitter(data_np, distributions=distribution_list)
                    f.fit()
                    best_fit = f.get_best(method="sumsquare_error")
                    # print(f"{best_fit}")

                    for key in best_fit:
                        # print(key)
                        params_list.append(key)
                        for nest_key in best_fit.get(key):
                            # print(best_fit.get(key).get(nest_key))
                            params_list.append(best_fit.get(key).get(nest_key))
                    
                    tmp_list = distribution_generate(params_list, amount_generate)
                    tmp_list = [round(num) for num in tmp_list]
                    tmp_list = [round(num, 4) for num in tmp_list]
                    # print(f"{params_list}, \n")
            
            elif ((i["type"].lower() == "double" or i["type"].lower() == "float") and "foreign key" not in i["metadata"] and "primary key" not in i["metadata"] and i["name"] != "specobjid"):
                    # print(df_table[i["name"]])
                    # print("numbers")
                    data_np = df_table[i["name"]].values

                    f = Fitter(data_np, distributions=distribution_list)
                    f.fit()
                    best_fit = f.get_best(method="sumsquare_error")
                    # print(f"{best_fit}")

                    for key in best_fit:
                        # print(key)
                        params_list.append(key)
                        for nest_key in best_fit.get(key):
                            # print(best_fit.get(key).get(nest_key))
                            params_list.append(best_fit.get(key).get(nest_key))
                    
                    data_list = distribution_generate(params_list, amount_generate)
                    # print(f"{params_list}, \n")

            
            for j in range(1, amount_generate + 1):
                if ("primary key" in i["metadata"]):
                    data_list.append(j)
                
                elif ("foreign key" in i["metadata"] or i["name"] == "specobjid"):
                    data_list = list(range(1, amount_generate + 1))
                    random.shuffle(data_list)
                    # adding a break, since a complete list is generated here therefore unnecessary to do this multiple times.
                    break
                    
                elif (i["type"].lower() == "string" or i["type"].lower() == "varchar"):
                    data_list.append(fake.pystr(min_chars=1, max_chars=i["metadata"]["varchar"]))

            # if ("primary key" in i["metadata"]):
            #     print("Primary Key")
                
            # elif ("foreign key" in i["metadata"]):
            #     print("Foreign Key")
                    
            # elif (i["type"].lower() == "string" or i["type"].lower() == "varchar"):
            #     print("Text")
                    
    
    return data_list



def generate_table_data(table_name, generate_number, dir_tables, dir_schema, dir_output):
    # print(dir_schema)

    file_table = (dir_tables / table_name).as_posix() + ".csv"
    file_schema = (dir_schema / table_name).as_posix() + ".json"
    print(file_table)
    table_df = pd.read_csv(file_table)
    attribute_list = list()
    schema_list = list()
    table_dict = dict()

    table_df.columns = table_df.columns.str.lower()


    with open(file_table) as csv_file, open(file_schema) as json_file:
        csv_dict = csv.DictReader(csv_file)
        json_dict = json.load(json_file)

        attribute_list = csv_dict.fieldnames
        schema_list = json_dict["fields"]
    
    # Change this, should be simplified.
    for i in range(len(attribute_list)):
        attribute_list[i] = attribute_list[i].lower()
    
    # Changing the keys to lowercase in the dictionary metadata.
    for i in range(len(schema_list)):
        schema_list[i]["name"] = schema_list[i]["name"].lower()
        schema_list[i]["metadata"] = dict((key.lower(), value) for key, value in schema_list[i]["metadata"].items())
    
    # table_dict[attribute_list[0]] = generate_data(attribute_list[0], file_table, schema_list, generate_number)

    # print(table_dict)

    # Adding column name as Key and generated data into a list as Value to the dictionary table_dict.
    for i in attribute_list:
        # table_dict[i] = generate_data2(i, table_df, schema_list, generate_number)
        table_dict[i] = generate_data2(i, table_df.loc[0:100, : ], schema_list, generate_number)
    
    # print(table_dict)

    # pd.set_option("max_columns", None)
    # Create a df based on the dictionary table_dict and save this df as a csv file.
    df_test = pd.DataFrame(table_dict)
    # print(df_test)
    df_test.to_csv(dir_output.as_posix() + f"/{table_name}.csv", index=False)



def update_synthetic_table(table, correlation_table_name, column_names, dir_config):

    table_csv = (dir_config.root / dir_config.tables_generated / table).as_posix() + ".csv"
    corr_table = (dir_config.root / dir_config.correlations  / f"{correlation_table_name}.csv").as_posix()

    # Create empty List and Dictionary which will be filled to rename the column names of df_corr_columns such that the columns
    # of both df's have the same name and the values of df_corr_columns can be added to df_table.
    columns = []
    rename_dict = {}
    for i in column_names:
        name = table + "_" + i.lower()
        columns.append(name)
        rename_dict[name] = i.lower()
    
    # print(columns)
    # print(rename_dict)

    # Importing complete table.
    df_table = pd.read_csv(table_csv)
    # print(df_table)
    # print(df_table[list(rename_dict.values())])
    # print(df_table.dtypes[list(rename_dict.values())])
    
    # Import the specific columns from the table containing the correlated attributes and renaming the column names.
    df_corr_columns = pd.read_csv(corr_table, usecols=columns).rename(columns=rename_dict)
    # print(df_corr_columns)
    print("\nMatrix:")
    print(df_corr_columns.dtypes)

    for i in rename_dict.values():
        df_corr_columns[i] = df_corr_columns[i].astype(df_table.dtypes[i])

    print("\nMatrix Changed:")
    print(df_corr_columns.dtypes)

    df_table.update(df_corr_columns, overwrite=True)

    # Check wether the columns are replaced.
    print("Are the df's the same:", df_table[list(rename_dict.values())].equals(df_corr_columns[:len(df_table.index)]))

    # Save the df containing correlated data.
    df_table.to_csv(table_csv, float_format="%g", header=True, index=False)
    
    



def main(config):
    output_logs = config.path.root / config.path.logs
    output_tables = config.path.root / config.path.tables
    schemas = config.path.root / config.path.schema
    correlation_dir = config.path.root / config.path.correlations
    generated_dir = config.path.root / config.path.tables_generated

    ##############################
    # Generating Synthetic Data. #
    ##############################

    # print("Output:", config.path.root)
    # generate_table_data("zoospec", 98410, output_tables, schemas, generated_dir)

    # generate_table_data("photoobjall", 100000, output_tables, schemas, generated_dir)
    # generate_table_data("photoobj", 100000, output_tables, schemas, generated_dir)
    # generate_table_data("specphotoall", 100000, output_tables, schemas, generated_dir)
    # generate_table_data("specphoto", 100000, output_tables, schemas, generated_dir)
    # generate_table_data("spplines", 100000, output_tables, schemas, generated_dir)
    # generate_table_data("sppparams", 50000, output_tables, schemas, generated_dir)
    # generate_table_data("wise_xmatch", 100000, output_tables, schemas, generated_dir)
    # generate_table_data("phototag", 100000, output_tables, schemas, generated_dir)
    # generate_table_data("galaxytag", 87499, output_tables, schemas, generated_dir)
    # generate_table_data("zoospec", 98410, output_tables, schemas, generated_dir)
    # generate_table_data("photoz", 100000, output_tables, schemas, generated_dir)
    # generate_table_data("apogeestar", 23, output_tables, schemas, generated_dir)
    # generate_table_data("galaxy", 100000, output_tables, schemas, generated_dir)
    # generate_table_data("galspecextra", 73905, output_tables, schemas, generated_dir)
    # generate_table_data("galspecindx", 96101, output_tables, schemas, generated_dir)
    # generate_table_data("galspecline", 96101, output_tables, schemas, generated_dir)
    # generate_table_data("stellarmassfspsgranearlydust", 96842, output_tables, schemas, generated_dir)
    # generate_table_data("mangagalaxyzoo", 4220, output_tables, schemas, generated_dir)
    # generate_table_data("mangadrpall", 4220, output_tables, schemas, generated_dir)
    # generate_table_data("mangapipe3d", 4534, output_tables, schemas, generated_dir)

    
    ####################################################################
    # Replacing columns of the generated tables with columns generated #
    # using the correlation matrix                                     #
    ####################################################################

    # update_synthetic_table("galaxy",
    #     "qcs_attributes_generated",
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
    #     config.path)

    # update_synthetic_table("galaxytag",
    #     "qcs_attributes_generated",
    #     ["dec",
    #     "ra",
    #     "type"],
    #     config.path)

    # update_synthetic_table("galspecextra",
    #     "qcs_attributes_generated",
    #     ["bptclass",
    #     "sfr_fib_p50",
    #     "sfr_tot_p50",
    #     "sfr_tot_p84",
    #     "specsfr_tot_p50"],
    #     config.path)

    # update_synthetic_table("galspecindx",
    #     "qcs_attributes_generated",
    #     ["d4000_n"],
    #     config.path)

    # update_synthetic_table("galspecline",
    #     "qcs_attributes_generated",
    #     ["h_alpha_eqw",
    #     "h_alpha_flux",
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
    #     config.path)

    # update_synthetic_table("photoobj",
    #     "qcs_attributes_generated",
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
    #     config.path)

    # update_synthetic_table("photoobjall",
    #     "qcs_attributes_generated",
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
    #     config.path)
    
    # update_synthetic_table("phototag",
    #     "qcs_attributes_generated",
    #     ["clean",
    #     "dec",
    #     "mode",
    #     "nChild",
    #     "psfMag_r",
    #     "ra",
    #     "type"],
    #     config.path)

    # update_synthetic_table("photoz",
    #     "qcs_attributes_generated",
    #     ["absMagR",
    #     "photoErrorClass",
    #     "nnCount",
    #     "nnVol",
    #     "z",
    #     "zErr"],
    #     config.path)

    # update_synthetic_table("specphoto",
    #     "qcs_attributes_generated",
    #     ["dec",
    #     "mode",
    #     "modelMag_r",
    #     "petroMag_r",
    #     "petroMag_z",
    #     "ra",
    #     "type",
    #     "z",
    #     "zWarning"],
    #     config.path)

    # update_synthetic_table("sppparams",
    #     "qcs_attributes_generated",
    #     ["FEHADOP"],
    #     config.path)

    # update_synthetic_table("stellarmassfspsgranearlydust",
    #     "qcs_attributes_generated",
    #     ["logMass",
    #     "z"],
    #     config.path)
    
    # update_synthetic_table("zoospec",
    #     "qcs_attributes_generated",
    #     ["elliptical",
    #     "p_cs",
    #     "p_cs_debiased",
    #     "p_el",
    #     "p_el_debiased",
    #     "spiral",
    #     "uncertain"],
    #     config.path)


if __name__=='__main__':
    config = get_config("txe")
    main(config=config)