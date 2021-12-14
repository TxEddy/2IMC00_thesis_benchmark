from config import get_config
from pathlib import Path
import pandas as pd

# Export Primary Keys of PhotoObjAll as csv.
def export_pk_photoobjall(csv_table, output_dir):
    table_name = csv_table.as_posix().split("/")[-1].split(".")[0]
    df = pd.read_csv(csv_table)
    df.columns = df.columns.str.lower()
    df[['objid','specobjid']].to_csv((output_dir / f"{table_name}_export_pk_fk.csv").as_posix(), header=True, index=False)


def get_keys(csv_keys):
    df = pd.read_csv(csv_keys)

    objid_list = df['objid'].values.tolist()
    specobjid_list = df['specobjid'].values.tolist()

    return objid_list, specobjid_list


def iterate_tables(table_dir, output_dir):
    tables = table_dir.glob('*.csv') # Also get csv files in underlying folders.
    paths_tables_csv = [path.as_posix() for path in tables]

    # for i in paths_tables_csv:
    #     print(i)

    # replacing_fks(paths_tables_csv[8], "test")


def replace_fk_values(table_column_list, fk_list):
    missing_values_list = []
    index = 0

    # Create list with missing values based on the main table.
    for i in range(0, len(fk_list)):
        if fk_list[i] not in table_column_list:
            missing_values_list.append(fk_list[i])
    
    # Making a set of the list and back to remove duplicate values.
    missing_values_set = set(missing_values_list)
    missing_values_list = list(missing_values_set)
    missing_values_list.sort()

    # Replacing values which are not in the main table using the list having the missing values.
    for i in range(0, len(table_column_list)):
        if table_column_list[i] not in fk_list:
            # print(f"{table_column_list[i]} is not in list, index is: {i}")
            # while missing_values_list[index] in table_column_list:
            #     index = index + 1
            #     print(f"Found Duplicate, index is: {index}")
            # print("Pickle Rick")
                
            table_column_list[i] = missing_values_list[index]
            index = index + 1
    
    return table_column_list


def replacing_fks(table_csv, pk_csv):
    print(table_csv)
    # Get the main joining keys of the columns: 'objID' and 'specObjID'.
    objid_pk_list, specobjid_pk_list = get_keys(pk_csv)

    # Read the input table as pandas DataFrame and change their column names to lowercase.
    df_table = pd.read_csv(table_csv)
    df_table.columns = df_table.columns.str.lower()
    df_specobjid_list = []
    df_objid_list = []
    objid_alias = ""

    # Check whether the column names 'objid' or 'bestobjid' exists and get their values as a list.
    if 'objid' in df_table.columns:
        df_objid_list = df_table['objid'].values.tolist()
        objid_alias = "objid"
    elif 'bestobjid' in df_table.columns:
        df_objid_list = df_table['bestobjid'].values.tolist()
        objid_alias = "bestobjid"
    elif 'sdss_objid' in df_table.columns:
        df_objid_list = df_table['sdss_objid'].values.tolist()
        objid_alias = "sdss_objid"

    # Check whether the column name 'specobjid' exists and get their values as a list.
    if 'specobjid' in df_table.columns:
        df_specobjid_list = df_table['specobjid'].values.tolist()

    # Testing Number of records specObjID.
    specobjid_set = set(specobjid_pk_list)
    specobjid_intersection1 = specobjid_set.intersection(df_specobjid_list)
    print(f"specObjID common values: {len(specobjid_intersection1)}")

    # Replacing 'specObjID's which are not in the main table.
    df_specobjid_list = replace_fk_values(df_specobjid_list, specobjid_pk_list)

    # Testing Number of records SpecObjID after replacing "missing" values.
    specobjid_intersection2 = specobjid_set.intersection(df_specobjid_list)
    print(f"specObjID common values after replacing missing values: {len(specobjid_intersection2)}")
    print(f"Length of specObjID list: {len(df_specobjid_list)}")
    tmp_set1 = set(df_specobjid_list)
    print(f"Length of specObjID list without duplicates: {len(tmp_set1)}")

    print("-----------------------------------------------------------------")

    # Testing Number of records objID.
    objid_set = set(objid_pk_list)
    objid_intersection1 = objid_set.intersection(df_objid_list)
    print(f"objID common values: {len(objid_intersection1)}")
    
    # Replacing 'objID's which are not in the main table.
    df_objid_list = replace_fk_values(df_objid_list, objid_pk_list)
    
    # Testing Number of records objID after replacing "missing" values.
    objid_intersection2 = objid_set.intersection(df_objid_list)
    print(f"objID common values after replacing missing values: {len(objid_intersection2)}")
    print(f"Length of objID list: {len(df_objid_list)}")
    tmp_set2 = set(df_objid_list)
    print(f"Length of objID list without duplicates: {len(tmp_set2)}")

    # Creating temporary DataFrame having 'objid' and 'specobjid' as columns.
    # df_tmp = pd.DataFrame(list(zip(df_objid_list, df_specobjid_list)), columns=[objid_alias, 'specobjid'])
    df_objid_tmp = pd.DataFrame(df_objid_list, columns=[objid_alias])
    df_specobjid_tmp = pd.DataFrame(df_specobjid_list, columns=['specobjid'])
    # print(df_objid_tmp)
    # print(df_specobjid_tmp)

    # Replacing 'objid' and 'specobjid' columns of the input table with new values.
    # df_table['specobjid'] = df_tmp['specobjid']
    # df_table['objid'] = df_tmp['objid']

    # Check whether the column names 'objid' or 'bestobjid' exists and replace with new values.
    if 'objid' in df_table.columns:
        df_table['objid'] = df_objid_tmp[objid_alias]
    elif 'bestobjid' in df_table.columns:
        df_table['bestobjid'] = df_objid_tmp[objid_alias]
    elif 'sdss_objid' in df_table.columns:
        df_table['sdss_objid'] = df_objid_tmp[objid_alias]

    # Check whether the column name 'specobjid' exists and replace with new values.
    if 'specobjid' in df_table.columns:
        df_table['specobjid'] = df_specobjid_tmp['specobjid']

    # Saving the changed table.
    df_table.to_csv(table_csv, header=True, index=False)

    




def main(config):
    output_logs = config.path.root / config.path.logs
    output_tables = config.path.root / config.path.tables
    schemas = config.path.root / config.path.schema
    correlation_dir = config.path.root / config.path.correlations
    generated_dir = config.path.root / config.path.tables_generated

    #######################################
    # Exporting primary keys PhotoObjAll. #
    #######################################
    table_photoobjall = output_tables / "photoobjall.csv"
    export_pk_photoobjall(table_photoobjall, config.path.root)


    ########################################################################
    # Replace missing primary/foreign keys with keys of PhotoObjAll table. #
    ########################################################################
    photooobjall_fk_pk = output_tables / "photoobjall_export_pk_fk.csv"

    table_order_list = ['specphotoall',
                        'specphoto',
                        'phototag',
                        'spplines',
                        'sppparams',
                        'wise_xmatch',
                        'zoospec',
                        'photoz',
                        'galaxytag',
                        'galaxy',
                        'galspecextra',
                        'galspecindx',
                        'galspecline',
                        'stellarmassfspsgranearlydust']
    
    for i in table_order_list:
        # print(f"{(output_tables / i).as_posix()}.csv")
        replacing_fks(f"{(output_tables / i).as_posix()}.csv", photooobjall_fk_pk)
    
    
    # Replacing pk and fk of galaxytag using phototag, when fk of photootags have been replaced.
    # Only galaxytag.
    
    #####################################################################
    # Replace missing primary/foreign keys with keys of PhotoTag table. #
    #####################################################################
    # table_phototag = output_tables / "phototag.csv"
    # export_pk_photoobjall(table_phototag, output_tables)


    ##################################################################################
    # Replace missing primary/foreign keys of galaxyTag with keys of PhotoTag table. #
    ##################################################################################
    # phototag_fk_pk = output_tables / "phototag_export_pk_fk.csv"
    # replacing_fks(f"{(output_tables / 'galaxytag').as_posix()}.csv", phototag_fk_pk)
    


    # print(f"{(output_tables / table_order_list[0]).as_posix()}.csv")
    # print(table_order_list[5])
    # replacing_fks(f"{(output_tables / table_order_list[3]).as_posix()}.csv", photooobjall_fk_pk)
    # replacing_fks(f"{(output_tables / table_order_list[5]).as_posix()}.csv", photooobjall_fk_pk)
    # replacing_fks(f"{(output_tables / table_order_list[7]).as_posix()}.csv", photooobjall_fk_pk)
    # replacing_fks(f"{(output_tables / table_order_list[-1]).as_posix()}.csv", photooobjall_fk_pk)
    
    # replacing_fks(paths_tables_csv[9], photooobjall_fk_pk)
    # replacing_fks(paths_tables_csv[17], photooobjall_fk_pk)




if __name__=='__main__':
    config = get_config("txe")
    main(config=config)