import wget, csv, calendar, re
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

    # regex for matching single line containing the end of a record.
    test1 = r"(\,0\,\,1\n)"

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
    with open(csv_download,'rb') as downloaded_file, open(csv_file, 'w', newline='') as file_:
        # Skipping the first line for table data or query logs.
        if not qry_log:
            # Skip the first line of the table data, which is just the query statement.
            next(downloaded_file)
        else:
            # Skip the first line which is the header of the csv file and write immediately to new csv file.
            header = next(downloaded_file)
            file_.write(header.decode('utf-8'))

        for line in downloaded_file:
            # When the file contains query log data, remove \r\n (crlf), which are unnecessary whitelines/spacees.
            if qry_log:
                line = line.replace(b"\r\n", b" ")
                
                # match = re.match(test1, line.decode('utf-8'))
                match = re.match(test1, line.decode('windows-1252'))
                if bool(match) == False:
                    line = line.replace(b"\n", b" ")
                    line = line.replace(b",0,,1 ", b",0,,1\n")
                
                file_.write(line.decode('windows-1252'))
            
            else:
                # Write row of table data to new CSV file.
                file_.write(line.decode('utf-8'))
    
    # TEST CODE FOR REMOVING CRLF (TRAILING STATEMENTS) FROM THE FILE.
    # with open(csv_download, "rb") as inf:
    #     with open(csv_file, "w") as fixed:
    #         for line in inf:
    #             line = line.replace(b"\r\n", b" ")

    #             # match = re.match(test1, line.decode('utf-8'))
    #             match = re.match(test1, line.decode('utf-8'))

    #             if bool(match) == False:
    #                 line = line.replace(b"\n", b" ")
    #                 line = line.replace(b",0,,1 ", b",0,,1\n")
                
    #             # second_match = re.match(test2, line.decode('utf-8'))

    #             # if bool(second_match):
    #             #     line = line.replace(b",0,,1 ", b",0,,1\n")


    #             # print(line)
                
    #             fixed.write(line.decode('utf-8'))


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

    # TESTING FOR QCS WITH DATA AND LOGS FOR SPECIFIC TABLES.
    # skyserver_csv_download("SELECT top 1100 p.* FROM photoobjall p JOIN specobjall s on s.bestobjid = p.objid", "qcs_photo_obj_all", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT top 1100 s.* FROM specobjall s JOIN photoobjall p on p.objid = s.bestobjid", "qcs_spec_obj_all", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT top 5 * FROM SqlLog WHERE yy = 2020 and mm = 10 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%photoobjall%' or lower(statement) like '%specobjall%') and (lower(statement) not like '%fget%')", "qcs_test", dir_logs)
    

    # Downloading the query log of the year 2020.
    # skyserver_csv_download("SELECT * FROM SqlLog WHERE yy = 2020 and access like 'Skyserver.Search%' and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%' or lower(statement) like '%group by%') and lower(statement) like '%join%'", "log_2020", dir_logs)

    # Downloading the query log of the year 2020 having the top20 tables.
    # skyserver_csv_download("SELECT * FROM SqlLog WHERE yy = 2020 and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%' or lower(statement) like '%group by%') and lower(statement) like '%join%' and (lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%specphoto%' or lower(statement) like '%stellarmassfspsgranearlydust%' or lower(statement) like '%photoobjall%' or lower(statement) like '%mangapipe3d%' or lower(statement) like '%mangadrpall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%spplines%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoz%' or lower(statement) like '%sppparams%' or lower(statement) like '%galspecline%' or lower(statement) like '%galspecindx%' or lower(statement) like '%zoospec%' or lower(statement) like '%apogeestar%' or lower(statement) like '%phototag%' or lower(statement) like '%galspecextra%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%mangagalaxyzoo%') and lower(statement) not like '%fget%'", "log_2020_filter", dir_logs)

    # Downloading table data.
    # skyserver_csv_download("SELECT distinct top 15000 poa.* FROM photoobjall poa JOIN photoobj po on po.objid = poa.objid JOIN galaxy g on g.objid = poa.objid JOIN specphoto sp on sp.objid = poa.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid JOIN specphotoall spa on spa.objid = poa.objid JOIN spplines sl on sl.bestobjid = poa.objid JOIN galaxytag gt on gt.objid = poa.objid JOIN photoz pz on pz.objid = poa.objid JOIN sppparams sps on sps.bestobjid = poa.objid JOIN galspecline gs on gs.specobjid = poa.specobjid JOIN galspecindx gsi on gsi.specobjid = poa.specobjid JOIN zoospec zs on zs.objid = poa.objid JOIN phototag pt on pt.objid = poa.objid JOIN galspecextra gse on gse.specobjid = poa.specobjid JOIN wise_xmatch w on w.sdss_objid = poa.objid ORDER BY poa.objid asc", "photoobjall", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 po.* FROM photoobj po JOIN photoobjall poa on poa.objid = po.objid JOIN galaxy g on g.objid = po.objid JOIN specphoto sp on sp.objid = po.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = po.specobjid JOIN specphotoall spa on spa.specobjid = po.specobjid JOIN spplines sl on sl.bestobjid = po.objid JOIN galaxytag gt on gt.objid = po.objid JOIN photoz pz on pz.objid = po.objid JOIN sppparams sps on sps.bestobjid = po.objid JOIN galspecline gs on gs.specobjid = po.specobjid JOIN galspecindx gsi on gsi.specobjid = po.specobjid JOIN zoospec zs on zs.objid = po.objid JOIN phototag pt on pt.objid = po.objid JOIN galspecextra gse on gse.specobjid = po.specobjid JOIN wise_xmatch w on w.sdss_objid = po.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "photoobj", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 g.* FROM galaxy g JOIN photoobjall poa on poa.objid = g.objid JOIN photoobj po on po.objid = g.objid JOIN specphoto sp on sp.objid = g.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = g.specobjid JOIN specphotoall spa on spa.objid = g.objid JOIN spplines sl on sl.bestobjid = g.objid JOIN galaxytag gt on gt.objid = g.objid JOIN photoz pz on pz.objid = g.objid JOIN sppparams sps on sps.bestobjid = g.objid JOIN galspecline gs on gs.specobjid = g.specobjid JOIN galspecindx gsi on gsi.specobjid = g.specobjid JOIN zoospec zs on zs.objid = g.objid JOIN phototag pt on pt.objid = g.objid JOIN galspecextra gse on gse.specobjid = g.specobjid JOIN wise_xmatch w on w.sdss_objid = g.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "galaxy", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 sp.* FROM specphoto sp JOIN photoobjall poa on poa.objid = sp.objid JOIN galaxy g on g.objid = sp.objid JOIN photoobj po on po.objid = sp.objid JOIN specphotoall spa on spa.objid = sp.objid JOIN spplines sl on sl.bestobjid = sp.objid JOIN galaxytag gt on gt.objid = sp.objid JOIN sppparams sps on sps.bestobjid = sp.objid JOIN galspecline gs on gs.specobjid = sp.specobjid JOIN galspecindx gsi on gsi.specobjid = sp.specobjid JOIN zoospec zs on zs.objid = sp.objid JOIN phototag pt on pt.objid = sp.objid JOIN galspecextra gse on gse.specobjid = sp.specobjid JOIN wise_xmatch w on w.sdss_objid = sp.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "specphoto", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 smfged.* FROM stellarmassfspsgranearlydust smfged JOIN photoobjall poa on poa.specobjid = smfged.specobjid JOIN galaxy g on g.specobjid = smfged.specobjid JOIN specphoto sp on sp.specobjid = smfged.specobjid JOIN specphotoall spa on spa.specobjid = smfged.specobjid JOIN spplines sl on sl.specobjid = smfged.specobjid JOIN galaxytag gt on gt.specobjid = smfged.specobjid JOIN sppparams sps on sps.specobjid = smfged.specobjid JOIN galspecline gs on gs.specobjid = smfged.specobjid JOIN galspecindx gsi on gsi.specobjid = smfged.specobjid JOIN zoospec zs on zs.specobjid = smfged.specobjid JOIN phototag pt on pt.specobjid = smfged.specobjid JOIN galspecextra gse on gse.specobjid = smfged.specobjid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "stellarmassfspsgranearlydust", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 * FROM mangapipe3d", "mangapipe3d", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 * FROM mangadrpall ORDER BY mangaid", "mangadrpall", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 spa.* FROM specphotoall spa JOIN photoobjall poa on poa.objid = spa.objid JOIN photoobj po on po.objid = spa.objid JOIN galaxy g on g.objid = spa.objid JOIN specphoto sp on sp.objid = spa.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = spa.specobjid JOIN spplines sl on sl.bestobjid = spa.objid JOIN galaxytag gt on gt.objid = spa.objid JOIN photoz pz on pz.objid = spa.objid JOIN sppparams sps on sps.bestobjid = spa.objid JOIN galspecline gs on gs.specobjid = spa.specobjid JOIN galspecindx gsi on gsi.specobjid = spa.specobjid JOIN zoospec zs on zs.objid = spa.objid JOIN phototag pt on pt.objid = spa.objid JOIN galspecextra gse on gse.specobjid = spa.specobjid JOIN wise_xmatch w on w.sdss_objid = spa.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "specphotoall", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 sl.* FROM spplines sl JOIN photoobjall poa on poa.objid = sl.bestobjid JOIN photoobj po on po.objid = sl.bestobjid JOIN galaxy g on g.objid = sl.bestobjid JOIN specphoto sp on sp.objid = sl.bestobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = sl.specobjid JOIN specphotoall spa on spa.objid = sl.bestobjid JOIN galaxytag gt on gt.objid = sl.bestobjid JOIN photoz pz on pz.objid = sl.bestobjid JOIN sppparams sps on sps.bestobjid = sl.bestobjid JOIN galspecline gs on gs.specobjid = sl.specobjid JOIN galspecindx gsi on gsi.specobjid = sl.specobjid JOIN zoospec zs on zs.objid = sl.bestobjid JOIN phototag pt on pt.objid = sl.bestobjid JOIN galspecextra gse on gse.specobjid = sl.specobjid JOIN wise_xmatch w on w.sdss_objid = sl.bestobjid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "spplines", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 gt.* FROM galaxytag gt JOIN photoobjall poa on poa.objid = gt.objid JOIN photoobj po on po.objid = gt.objid JOIN galaxy g on g.objid = gt.objid JOIN specphoto sp on sp.objid = gt.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gt.specobjid JOIN specphotoall spa on spa.objid = gt.objid JOIN spplines sl on sl.bestobjid = gt.objid JOIN photoz pz on pz.objid = gt.objid JOIN sppparams sps on sps.bestobjid = gt.objid JOIN zoospec zs on zs.objid = gt.objid JOIN phototag pt on pt.objid = gt.objid JOIN galspecextra gse on gse.specobjid = gt.specobjid JOIN wise_xmatch w on w.sdss_objid = gt.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "galaxytag", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 pz.* FROM photoz pz JOIN photoobjall poa on poa.objid = pz.objid JOIN photoobj po on po.objid = pz.objid JOIN galaxy g on g.objid = pz.objid JOIN specphoto sp on sp.objid = pz.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid JOIN specphotoall spa on spa.objid = pz.objid JOIN spplines sl on sl.bestobjid = pz.objid JOIN galaxytag gt on gt.objid = pz.objid JOIN sppparams sps on sps.bestobjid = pz.objid JOIN galspecline gs on gs.specobjid = poa.specobjid JOIN galspecindx gsi on gsi.specobjid = poa.specobjid JOIN zoospec zs on zs.objid = poa.objid JOIN phototag pt on pt.objid = pz.objid JOIN galspecextra gse on gse.specobjid = poa.specobjid JOIN wise_xmatch w on w.sdss_objid = pz.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "photoz", dir_tables, qry_log=False)
    skyserver_csv_download("SELECT distinct top 15000 sps.* FROM sppparams sps JOIN photoobjall poa on poa.objid = sps.bestobjid JOIN photoobj po on po.objid = sps.bestobjid JOIN galaxy g on g.objid = sps.bestobjid JOIN specphoto sp on sp.objid = sps.bestobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = sps.specobjid JOIN specphotoall spa on spa.objid = sps.bestobjid JOIN spplines sl on sl.bestobjid = sps.bestobjid JOIN galaxytag gt on gt.objid = sps.bestobjid JOIN photoz pz on pz.objid = sps.bestobjid JOIN galspecline gs on gs.specobjid = sps.specobjid JOIN galspecindx gsi on gsi.specobjid = sps.specobjid JOIN zoospec zs on zs.objid = sps.bestobjid JOIN phototag pt on pt.specobjid = sps.specobjid JOIN galspecextra gse on gse.specobjid = sps.specobjid JOIN wise_xmatch w on w.sdss_objid = sps.bestobjid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "sppparams", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 gs.* FROM galspecline gs JOIN photoobjall poa on poa.specobjid = gs.specobjid JOIN photoobj po on po.specobjid = gs.specobjid JOIN galaxy g on g.specobjid = gs.specobjid JOIN specphoto sp on sp.specobjid = gs.specobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gs.specobjid JOIN specphotoall spa on spa.specobjid = gs.specobjid JOIN spplines sl on sl.specobjid = gs.specobjid JOIN galaxytag gt on gt.specobjid = gs.specobjid JOIN photoz pz on pz.objid = poa.objid JOIN sppparams sps on sps.specobjid = gs.specobjid JOIN galspecindx gsi on gsi.specobjid = gs.specobjid JOIN zoospec zs on zs.specobjid = gs.specobjid JOIN phototag pt on pt.specobjid = gs.specobjid JOIN galspecextra gse on gse.specobjid = gs.specobjid JOIN wise_xmatch w on w.sdss_objid = poa.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "galspecline", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 gsi.* FROM galspecindx gsi JOIN photoobjall poa on poa.specobjid = gsi.specobjid JOIN galaxy g on g.specobjid = gsi.specobjid JOIN specphoto sp on sp.specobjid = gsi.specobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gsi.specobjid JOIN specphotoall spa on spa.specobjid = gsi.specobjid JOIN spplines sl on sl.specobjid = gsi.specobjid JOIN galaxytag gt on gt.specobjid = gsi.specobjid JOIN photoz pz on pz.objid = poa.objid JOIN sppparams sps on sps.specobjid = gsi.specobjid JOIN galspecline gs on gs.specobjid = gsi.specobjid JOIN zoospec zs on zs.specobjid = gsi.specobjid JOIN phototag pt on pt.specobjid = gsi.specobjid JOIN galspecextra gse on gse.specobjid = gsi.specobjid JOIN wise_xmatch w on w.sdss_objid = poa.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "galspecindx", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 zs.* FROM zoospec zs JOIN photoobjall poa on poa.objid = zs.objid JOIN photoobj po on po.objid = zs.objid JOIN galaxy g on g.objid = zs.objid JOIN specphoto sp on sp.objid = zs.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = zs.specobjid JOIN specphotoall spa on spa.objid = zs.objid JOIN spplines sl on sl.bestobjid = zs.objid JOIN galaxytag gt on gt.objid = zs.objid JOIN photoz pz on pz.objid = zs.objid JOIN sppparams sps on sps.bestobjid = zs.objid JOIN galspecline gs on gs.specobjid = zs.specobjid JOIN galspecindx gsi on gsi.specobjid = zs.specobjid JOIN phototag pt on pt.objid = zs.objid JOIN galspecextra gse on gse.specobjid = zs.specobjid JOIN wise_xmatch w on w.sdss_objid = zs.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "zoospec", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 asr.* FROM apogeestar asr JOIN photoobjall poa on poa.htmid = asr.htmid JOIN photoobj po on po.htmid = asr.htmid JOIN galaxy g on g.htmid = asr.htmid JOIN specphoto sp on sp.htmid = asr.htmid JOIN specphotoall spa on spa.htmid = asr.htmid JOIN galaxytag gt on gt.htmid = asr.htmid JOIN phototag pt on pt.htmid = asr.htmid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "apogeestar", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 pt.* FROM phototag pt JOIN photoobjall poa on poa.objid = pt.objid JOIN photoobj po on po.objid = pt.objid JOIN galaxy g on g.objid = pt.objid JOIN specphoto sp on sp.objid = pt.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = pt.specobjid JOIN specphotoall spa on spa.objid = pt.objid JOIN spplines sl on sl.bestobjid = pt.objid JOIN galaxytag gt on gt.objid = pt.objid JOIN photoz pz on pz.objid = pt.objid JOIN sppparams sps on sps.bestobjid = pt.objid JOIN galspecline gs on gs.specobjid = pt.specobjid JOIN galspecindx gsi on gsi.specobjid = pt.specobjid JOIN zoospec zs on zs.objid = pt.objid JOIN galspecextra gse on gse.specobjid = pt.specobjid JOIN wise_xmatch w on w.sdss_objid = pt.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "phototag", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 gse.* FROM galspecextra gse JOIN photoobjall poa on poa.specobjid = gse.specobjid JOIN photoobj po on po.specobjid = gse.specobjid JOIN galaxy g on g.specobjid = gse.specobjid JOIN specphoto sp on sp.specobjid = gse.specobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gse.specobjid JOIN specphotoall spa on spa.specobjid = gse.specobjid JOIN spplines sl on sl.specobjid = gse.specobjid JOIN galaxytag gt on gt.specobjid = gse.specobjid JOIN photoz pz on pz.objid = poa.objid JOIN sppparams sps on sps.specobjid = gse.specobjid JOIN galspecline gs on gs.specobjid = gse.specobjid JOIN galspecindx gsi on gsi.specobjid = gse.specobjid JOIN zoospec zs on zs.specobjid = gse.specobjid JOIN phototag pt on pt.specobjid = gse.specobjid JOIN wise_xmatch w on w.sdss_objid = poa.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "galspecextra", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 w.* FROM wise_xmatch w JOIN photoobjall poa on poa.objid = w.sdss_objid JOIN photoobj po on po.objid = w.sdss_objid JOIN galaxy g on g.objid = w.sdss_objid JOIN specphoto sp on sp.objid = w.sdss_objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid JOIN specphotoall spa on spa.objid = w.sdss_objid JOIN spplines sl on sl.bestobjid = w.sdss_objid JOIN galaxytag gt on gt.objid = w.sdss_objid JOIN photoz pz on pz.objid = w.sdss_objid JOIN sppparams sps on sps.bestobjid = w.sdss_objid JOIN galspecline gs on gs.specobjid = poa.specobjid JOIN galspecindx gsi on gsi.specobjid = poa.specobjid JOIN zoospec zs on zs.objid = w.sdss_objid JOIN phototag pt on pt.objid = w.sdss_objid JOIN galspecextra gse on gse.specobjid = poa.specobjid WHERE poa.objid between 1237645941824356443 and 1237649921110442096", "wise_xmatch", dir_tables, qry_log=False)
    # skyserver_csv_download("SELECT distinct top 15000 mgz.* FROM mangagalaxyzoo mgz JOIN mangadrpall mda on mda.nsa_nsaid = mgz.nsa_id", "mangagalaxyzoo", dir_tables, qry_log=False)
    

if __name__=='__main__':
    config = get_config("txe")
    main(config=config)
    