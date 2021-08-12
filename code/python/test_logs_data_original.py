import wget, csv, calendar
from pathlib import Path
from config import get_config

# Original variable to use in the function
# url_log = f"http://skyserver.sdss.org/log/en/traffic/x_sql.asp?cmd={qry_web}&format=csv"

# URL in which the custom query will be added in order to be executed on skyserver.
# url_table = f"http://skyserver.sdss.org/dr16/en/tools/search/x_results.aspx?searchtool=SQL&TaskName=Skyserver.Search.SQL&syntax=NoSyntax&ReturnHtml=true&cmd={qry_web}&format=csv&TableName="

# New method specificly for downloading logs per day per month

def skyserver_csv_download(qry, name, output, qry_log=True):
    qry_repl = qry.replace(" ", "+")
    qry_web = qry_repl.replace("%", "%25")


    # When log URL has to be used 'type_log' has to be True, if False table data URL will be used
    url = {
        True: f"http://skyserver.sdss.org/log/en/traffic/x_sql.asp?cmd={qry_web}&format=csv",
        False: f"http://skyserver.sdss.org/dr16/en/tools/search/x_results.aspx?searchtool=SQL&TaskName=Skyserver.Search.SQL&syntax=NoSyntax&ReturnHtml=true&cmd={qry_web}&format=csv&TableName="
    }

    csv_output = output.as_posix()
    csv_file = f"{csv_output}/{name}.csv"

    # Download output of the query as csv in given directory.
    csv_download = wget.download(url[qry_log], out=csv_output)
    
    # Open downloaded csv and write to new csv file.
    with open(csv_download,'r', encoding='unicode_escape', newline='') as downloaded_file, open(csv_file, 'w', newline='') as file_:
        # Skip the first line when downloading table data.
        if not qry_log:
            next(downloaded_file)
        
        # Creating csv Dictionary Writer object.
        data = csv.DictReader(downloaded_file)

        # Creating csv Dictionary Writer object, using the fieldnames of the previous fieldnames and ignoring extra fields using the extrasaction parameter.
        writer = csv.DictWriter(file_, fieldnames=data.fieldnames, extrasaction='ignore')
        writer.writeheader()

        for row in data:
            # When the file contains query log data, remove the whitelines in the query statements.
            if qry_log:
                # print(row)
                
                # When the SQL statement is not empty, replace whitelines with spaces.
                if row.get("statement") is not None:
                    row["statement"] = row["statement"].replace("\r\n", " ")

                    # Write row of query log to new CSV file.
                    writer.writerow(row)
            
            else:
                # Write row of table data to new CSV file.
                writer.writerow(row)                    
    
    # Delete downloaded csv.
    Path(csv_download).unlink()


def skyserver_specific_log(month_number, log_dir):
    cal = calendar.Calendar()

    # Specifying output directory for the query logs.
    month_abbr = calendar.month_abbr[month_number].lower()
    output_dir = log_dir / f"logs_{month_abbr}"
    # print(month_abbr)
    # print(output_dir)

    # f"http://skyserver.sdss.org/log/en/traffic/x_sql.asp?cmd=SELECT+top+100000+*+FROM+SqlLog+WHERE+yy+=+2020+and+mm+=+10+and+dd+=+{month_number}+and+dbname+=+'BestDR16'+and+(access+like+'Skyserver.Search%25')+and+(rows+>+0+and+error+!=+1)+and+(lower(statement)+like+'select%25'+or+lower(statement)+like+'%25count(%25'or+lower(statement)++like+'%25avg(%25'+or+lower(statement)++like+'%25sum(%25'+or+lower(statement)+like+'%25join%25')+and+(lower(statement)+like+'%25phototag%25'+or+lower(statement)+like+'%25sdssebossfirefly%25'+or+lower(statement)+like+'%25photoobj%25'+or+lower(statement)+like+'%25galaxy%25'+or+lower(statement)+like+'%25photoprimary%25'+or+lower(statement)+like+'%25specphoto%25'+or+lower(statement)+like+'%25star%25'+or+lower(statement)+like+'%25galaxytag%25'+or+lower(statement)+like+'%25photoobjall%25'+or+lower(statement)+like+'%25specphotoall%25'+or+lower(statement)+like+'%25galspecline%25'+or+lower(statement)+like+'%25first%25'+or+lower(statement)+like+'%25emissionlinesport%25'+or+lower(statement)+like+'%25photoz%25'+or+lower(statement)+like+'%25spplines%25'+or+lower(statement)+like+'%25field%25'+or+lower(statement)+like+'%25apogeestar%25'+or+lower(statement)+like+'%25wise_xmatch%25'+or+lower(statement)+like+'%25sppparams%25'+or+lower(statement)+like+'%25stellarmassstarformingport%25')+and+(lower+(statement)+not+like+'%25fget%25')&format=csv"

    # for date in cal.itermonthdates(2020, month_number):
    #     if month_number != date.month:
    #         continue

    #     skyserver_csv_download(f"SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 10 and dd = {date.day} and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and (lower(statement) not like '%fget%')", f"log_{date.day}_{month_number}_21", output_dir)

    #     print(f"Day is: {date.day}")
    
    # Get all csv files in the given directory.
    # files_csv = dir.glob('**/*.csv') # Also get csv files in underlying folders.
    files_csv = output_dir.glob('*.csv')
    paths_csv = [path.as_posix() for path in files_csv]




def main(config):
    # Create output directories.
    dir_logs = config.path.root / config.path.logs
    dir_tables = config.path.root / config.path.tables
    # print("Directory Logs:", dir_logs)
    # print("Directory Tables:", dir_tables)

    # Test Queries for downloading.
    # skyserver_csv_download("select top 100000 * from SqlLog where yy = 2020 and mm = 12", "log_dec", dir_logs)
    # skyserver_csv_download("select top 100000 * from SqlLog where yy = 2020 and mm = 12 and lower(statement) like 'select%' and lower(statement) like '%from%'", "log_dec", dir_logs)
    # skyserver_csv_download("select top 100000 * from SqlLog where yy = 2020 and mm = 11 and lower(statement) like 'select%' and lower(statement) like '%from%'", "log_nov", dir_logs)
    # skyserver_csv_download("select top 100000 * from SqlLog where yy = 2020 and mm = 11 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1)", "log_nov", dir_logs)
    # skyserver_csv_download("select top 100000 * from SqlLog where yy = 2020 and mm = 10 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1)", "log_oct", dir_logs)
    # skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 10 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like 'select%' or lower(statement) like '%count(%'or lower(statement)  like '%avg(%' or lower(statement)  like '%sum(%' or lower(statement) like '%join%') and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and (lower (statement) not like '%fget%')", "log_oct", dir_logs)
    # skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 10 and dd = 1 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and (lower(statement) not like '%fget%')", "log_oct_01", dir_logs)
    # skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 10 and dd = 15 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and (lower(statement) not like '%fget%')", "log_oct_15", dir_logs)
    # skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 10 and dd = 30 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and (lower(statement) not like '%fget%')", "log_oct_30", dir_logs)

    # skyserver_csv_download("select top 100 * from PhotoObjAll", "photo_obj_all_tets", dir_tables, qry_log=False)

    # skyserver_specific_log(10, dir_logs)

    # Downloading the logs for the months October, November, December.
    # skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 10 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%' or lower(statement) like '%join%')", "log_oct", dir_logs)
    # skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 11 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%' or lower(statement) like '%join%')", "log_nov", dir_logs)
    # skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 12 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%' or lower(statement) like '%join%')", "log_dec", dir_logs)

    # Downloading the logs for the months October, November, December having the top20 tables.
    skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 10 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and (lower(statement) not like '%fget%')", "log_oct", dir_logs)
    # skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 11 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and (lower(statement) not like '%fget%')", "log_nov", dir_logs)
    # skyserver_csv_download("SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 12 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and (lower(statement) not like '%fget%')", "log_dec", dir_logs)

    # Downloading table data.
    # skyserver_csv_download("select top 100000 * from phototag", "photo_tag", dir_tables, qry_log=False)                   #DONE
    # skyserver_csv_download("select top 100000 * from sdssebossfirefly", "sdss_eboss_firefly", dir_tables, qry_log=False)  #DONE
    # skyserver_csv_download("select top 100000 * from photoobj", "photo_obj", dir_tables, qry_log=False)                   #
    # skyserver_csv_download("select top 5000 * from galaxy", "galaxy", dir_tables, qry_log=False)                          #DONE
    # skyserver_csv_download("select top 5000 * from photoprimary", "photo_primary", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from specphoto", "spec_photo", dir_tables, qry_log=False)                 #DONE
    # skyserver_csv_download("select top 5000 * from star", "star", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 5000 * from galaxytag", "galaxy_tag", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from photoobjall", "photo_obj_all", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from specphotoall", "spec_photo_all", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from galspecline", "gal_spec_line", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from first", "first", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from emissionlinesport", "emission_lines_port", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from photoz", "photoz", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from spplines", "spp_lines", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from field", "field", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from apogeestar", "apogee_star", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from wise_xmatch", "wise_xmatch", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from sppparams", "spp_params", dir_tables, qry_log=False)
    # skyserver_csv_download("select top 100000 * from stellarmassstarformingport", "stellar_mass_starforming_port", dir_tables, qry_log=False)

    

if __name__=='__main__':
    config = get_config("txe")
    main(config=config)
    