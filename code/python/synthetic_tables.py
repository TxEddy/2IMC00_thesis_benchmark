from config import get_config
from pathlib import Path
import csv, json, random
import pandas as pd
from scipy.stats import multivariate_normal as mvn
from faker import Faker


def add_data_to_table(table, column_names, dir_tables):
    table_csv = (dir_tables / table).as_posix() + ".csv"
    corr_qcs_table = (dir_tables / "correlatedAttributesQcs.csv").as_posix()
    
    # Create empty List and Dictionary which will be filled to rename the column names of df_corr_columns such that the columns
    # of both df's have the same name and the values of df_corr_columns can be added to df_table.
    columns = []
    rename_dict = {}
    for i in column_names:
        name = table + "_" + i.lower()
        columns.append(name)
        rename_dict[name] = i
    
    # Importing complete table and afterwards select the specific columns.
    df_table = pd.read_csv(table_csv)
    df_original_columns = df_table.filter(column_names, axis=1)
    
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
    df_new = pd.concat([df_original_columns, df_corr_columns], axis=0).drop_duplicates(keep='first').reset_index(drop=True)
    df_new = df_new[:len(df_table.index)]
    # print(df_new)
    
    # df_original_columns.update(df_new, overwrite=True)
    df_table.update(df_new, overwrite=True)

    # Check wether the columns are replaced.
    df_original_columns = df_table.filter(column_names, axis=1)
    print("Are the df's the same:", df_original_columns.equals(df_new))

    # Save the df containing correlated data.
    df_table.to_csv((dir_tables / table).as_posix() + "_new.csv", float_format="%g", header=True, index=False)


def correlation_matrix(qcs_tables_dir, generate_number):
    # Directory containing data output with all attributes stated in the QCS.
    dir_qcs_attributes = qcs_tables_dir / "qcsAttributesOutput"

    # Get all csv files in the given directory, this is the same directory as the output directory in Spark
    # to export the df containing data of all attributes states in the QCS. Since Spark exports the df 
    # with a random name, globally get the csv files in the given directory.
    files_csv = dir_qcs_attributes.glob('**/*.csv') # Also get csv files in underlying folders.
    paths_csv = [path.as_posix() for path in files_csv]

    # print(paths_csv)
    # print(paths_csv[0])

    # Spark exports one csv file, therefore the path of the file is on position 0.
    qry_df = pd.read_csv(paths_csv[0])
    # print(qry_df.head(5))
    # print(qry_df.dtypes)
    column_names = qry_df.columns


    # Create an correlation matrix based on the attributes using the Pearson method.
    corr_matrix = qry_df.corr(method='pearson')
    # print(corr_matrix)

    # Filling the correlation matrix with 0's when there is a NaN value and export this correlation matrix as csv.
    corr_matrix = corr_matrix.fillna(0)
    # corr_matrix.to_csv(qcs_tables_dir.parents[0].as_posix() + "/testMatrixGenerate/qcsQry_complete_matrix.csv")
    corr_matrix.to_csv(qcs_tables_dir.as_posix() + "/qcs_attributes_matrix.csv")

    # Generate data using the correlation matrix and multivariate normal distribution sampling.
    # generated_corr = mvn.rvs(mean=qry_df.mean().array, cov=corr_matrix, size=generate_number)
    generated_corr = mvn.rvs(mean=qry_df.median().array, cov=corr_matrix, size=generate_number)
    # print(generated_corr)

    # Adding the column names to the generated data and export the generated data as csv.
    generated_df = pd.DataFrame(data = generated_corr, columns = qry_df.columns)
    
    # for i in column_names:
    #     # df_corr_columns[i] = df_corr_columns[i].astype(df_table.dtypes[i])
    #     generated_df[i] = generated_df[i].astype(qry_df.dtypes[i])

    generated_df.to_csv(qcs_tables_dir.as_posix() + "/qcs_attributes_generated.csv", index=False)


def generate_data(attribute_name, df_table, table_schema, amount_generate):
    fake = Faker()

    data_list = list()
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
                    data_list = list(range(1, amount_generate + 1))
                    random.shuffle(data_list)
                    # adding a break, since a complete list is generated here therefore unnecessary to do this multiple times.
                    break
                    
                elif (i["type"].lower() == "integer" or i["type"].lower() == "long"):
                    if (specific_int == "tinyint"):
                        data_list.append(fake.pyint(max_value=127))
                    
                    else:
                        data_list.append(fake.pyint())
                
                elif (i["type"].lower() == "double" or i["type"].lower() == "float"):
                    data_list.append(fake.pyfloat())
                
                elif (i["type"].lower() == "string" or i["type"].lower() == "varchar"):
                    data_list.append(fake.pystr(min_chars=1, max_chars=i["metadata"]["varchar"]))
    
    return data_list


def generate_table_data(table_name, generate_number, dir_tables, dir_schema, dir_output):

    file_table = (dir_tables / table_name).as_posix() + ".csv"
    file_schema = (dir_schema / table_name).as_posix() + ".json"
    print(file_table)
    table_df = pd.read_csv(file_table)
    attribute_list = list()
    schema_list = list()
    table_dict = dict()


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
    
    # Adding column name as Key and generated data into a list as Value to the dictionary table_dict.
    for i in attribute_list:
        table_dict[i] = generate_data(i, table_df, schema_list, generate_number)
    
    # print(table_dict)

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

    # Importing complete table.
    df_table = pd.read_csv(table_csv)
    
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
    # Create output directories.
    # create_dirs(config)

    # Set output directories.
    output_tables = config.path.root / config.path.tables
    schemas = config.path.root / config.path.schema
    correlation_dir = config.path.root / config.path.correlations
    generated_dir = config.path.root / config.path.tables_generated

    ######################################################
    #  Creating and calculating the correlation matrix.  #
    ######################################################
    correlation_matrix(correlation_dir, 100000)

    ######################################
    #  Generating Synthetic table data.  #
    ######################################

    generate_table_data("photoobjall", 100000, output_tables, schemas, generated_dir)
    generate_table_data("photoobj", 100000, output_tables, schemas, generated_dir)
    generate_table_data("specphotoall", 100000, output_tables, schemas, generated_dir)
    generate_table_data("specphoto", 100000, output_tables, schemas, generated_dir)
    generate_table_data("spplines", 100000, output_tables, schemas, generated_dir)
    generate_table_data("sppparams", 50000, output_tables, schemas, generated_dir)
    generate_table_data("wise_xmatch", 100000, output_tables, schemas, generated_dir)
    generate_table_data("phototag", 100000, output_tables, schemas, generated_dir)
    generate_table_data("galaxytag", 87499, output_tables, schemas, generated_dir)
    generate_table_data("zoospec", 98410, output_tables, schemas, generated_dir)
    generate_table_data("photoz", 100000, output_tables, schemas, generated_dir)
    generate_table_data("apogeestar", 23, output_tables, schemas, generated_dir)
    generate_table_data("galaxy", 100000, output_tables, schemas, generated_dir)
    generate_table_data("galspecextra", 73905, output_tables, schemas, generated_dir)
    generate_table_data("galspecindx", 96101, output_tables, schemas, generated_dir)
    generate_table_data("galspecline", 96101, output_tables, schemas, generated_dir)
    generate_table_data("stellarmassfspsgranearlydust", 96842, output_tables, schemas, generated_dir)
    generate_table_data("mangagalaxyzoo", 4220, output_tables, schemas, generated_dir)
    generate_table_data("mangadrpall", 4220, output_tables, schemas, generated_dir)
    generate_table_data("mangapipe3d", 4534, output_tables, schemas, generated_dir)
    

    #########################################################################
    #  Slicing the correlation table and adding these rows to the original  #
    #  single tables and removing duplicates rows of the table.             #
    #########################################################################

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


    ######################################################################
    #  Replacing columns of the generated tables with columns generated  #
    #  using the correlation matrix.                                     #
    ######################################################################

    update_synthetic_table("galaxy",
        "qcs_attributes_generated",
        ["clean",
        "dec",
        "g",
        "petroMag_r",
        "petroMag_u",
        "petroR90_g",
        "petroR90_r",
        "petroRad_u",
        "r",
        "ra"],
        config.path)

    update_synthetic_table("galaxytag",
        "qcs_attributes_generated",
        ["dec",
        "ra",
        "type"],
        config.path)

    update_synthetic_table("galspecextra",
        "qcs_attributes_generated",
        ["bptclass",
        "sfr_fib_p50",
        "sfr_tot_p50",
        "sfr_tot_p84",
        "specsfr_tot_p50"],
        config.path)

    update_synthetic_table("galspecindx",
        "qcs_attributes_generated",
        ["d4000_n"],
        config.path)

    update_synthetic_table("galspecline",
        "qcs_attributes_generated",
        ["h_alpha_eqw",
        "h_alpha_flux",
        "h_alpha_flux_err",
        "h_beta_eqw",
        "h_beta_flux",
        "h_beta_flux_err",
        "nii_6584_flux",
        "oi_6300_flux_err",
        "oiii_5007_eqw",
        "oiii_5007_flux",
        "sii_6717_flux",
        "sii_6731_flux_err"],
        config.path)

    update_synthetic_table("photoobj",
        "qcs_attributes_generated",
        ["b",
        "camcol",
        "clean",
        "cModelMag_g",
        "dec",
        "deVRad_g",
        "deVRad_r",
        "fiberMag_r",
        "field",
        "flags",
        "fracDeV_r",
        "g",
        "l",
        "mode",
        "petroMag_r",
        "petroMag_z",
        "petroR50_g",
        "petroR50_r",
        "petroRad_g",
        "petroRad_r",
        "r",
        "ra",
        "run",
        "type",
        "u"],
        config.path)

    update_synthetic_table("photoobjall",
        "qcs_attributes_generated",
        ["camcol",
        "clean",
        "dec",
        "dered_r",
        "deVRad_r",
        "deVRadErr_r",
        "expRad_r",
        "field",
        "fracDeV_r",
        "mode",
        "petroMag_r",
        "ra",
        "run",
        "type",
        "u"],
        config.path)
    
    update_synthetic_table("phototag",
        "qcs_attributes_generated",
        ["clean",
        "dec",
        "mode",
        "nChild",
        "psfMag_r",
        "ra",
        "type"],
        config.path)

    update_synthetic_table("photoz",
        "qcs_attributes_generated",
        ["absMagR",
        "photoErrorClass",
        "nnCount",
        "nnVol",
        "z",
        "zErr"],
        config.path)

    update_synthetic_table("specphoto",
        "qcs_attributes_generated",
        ["dec",
        "mode",
        "modelMag_r",
        "petroMag_r",
        "petroMag_z",
        "ra",
        "type",
        "z",
        "zWarning"],
        config.path)

    update_synthetic_table("sppparams",
        "qcs_attributes_generated",
        ["FEHADOP"],
        config.path)

    update_synthetic_table("stellarmassfspsgranearlydust",
        "qcs_attributes_generated",
        ["logMass",
        "z"],
        config.path)
    
    update_synthetic_table("zoospec",
        "qcs_attributes_generated",
        ["elliptical",
        "p_cs",
        "p_cs_debiased",
        "p_el",
        "p_el_debiased",
        "spiral",
        "uncertain"],
        config.path)


if __name__=='__main__':
    config = get_config("txe")
    main(config=config)