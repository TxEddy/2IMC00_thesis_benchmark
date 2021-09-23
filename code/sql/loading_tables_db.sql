/* Login per db cli
Microsoft SQL Server: mssql -u sa -p ... -d db_benchmark -T 60 000
MySQL: mysql -u root -p... db_benchmark
PostgreSQL: psql -d db_benchmark -U eddy */

-- Code for importing all csv files per table in PostgreSQL commandline.
psql -d postgres -c "\copy public.photoobjall from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/photoobjall.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.photoobj from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/photoobj.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.specphotoall from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/specphotoall.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.specphoto from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/specphoto.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.spplines from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/spplines.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.sppparams from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/sppparams.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.wise_xmatch from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/wise_xmatch.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.phototag from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/phototag.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.galaxytag from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galaxytag.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.zoospec from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/zoospec.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.photoz from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/photoz.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.apogeestar from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/apogeestar.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.galaxy from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galaxy.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.galspecextra from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galspecextra.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.galspecindx from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galspecindx.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.galspecline from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galspecline.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.stellarmassfspsgranearlydust from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/stellarmassfspsgranearlydust.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.mangagalaxyzoo from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/mangagalaxyzoo.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.mangadrpall from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/mangadrpall.csv' csv header delimiter ','"
psql -d postgres -c "\copy public.mangapipe3d from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/mangapipe3d.csv' csv header delimiter ','"

-- Code for importing all csv files per table in MySQL commandline. EXTRA Info: docker cp PATH_CSV_FILE CONTAINER_NAME:/
load data infile '/tmp/testdata/photoobjall.csv' into table photoobjall fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/photoobj.csv' into table photoobj fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/specphotoall.csv' into table specphotoall fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/specphoto.csv' into table specphoto fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/spplines.csv' into table spplines fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/sppparams.csv' into table sppparams fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/wise_xmatch.csv' into table wise_xmatch fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/phototag.csv' into table phototag fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/galaxytag.csv' into table galaxytag fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/zoospec.csv' into table zoospec fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/photoz.csv' into table photoz fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/apogeestar.csv' into table apogeestar fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/galaxy.csv' into table galaxy fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/galspecextra.csv' into table galspecextra fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/galspecindx.csv' into table galspecindx fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/galspecline.csv' into table galspecline fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/stellarmassfspsgranearlydust.csv' into table stellarmassfspsgranearlydust fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/mangagalaxyzoo.csv' into table mangagalaxyzoo fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/mangadrpall.csv' into table mangadrpall fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/testdata/mangapipe3d.csv' into table mangapipe3d fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;

-- Code for importing all csv files per table in mssql.
/* When using SQL Sever with Docker do the following:
1. Create new folder in tmp
2. Copy the tables files to folder create above to the container using: docker cp PATH_CSV_FILES/. CONTAINER_NAME:/tmp/FOLDER_NAME_STEP1
*/
bulk insert photoobjall from '/tmp/original_data/photoobjall.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert photoobj from '/tmp/original_data/photoobj.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert specphotoall from '/tmp/original_data/specphotoall.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert specphoto from '/tmp/original_data/specphoto.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert spplines from '/tmp/original_data/spplines.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert sppparams from '/tmp/original_data/sppparams.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert wise_xmatch from '/tmp/original_data/wise_xmatch.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert phototag from '/tmp/original_data/phototag.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galaxytag from '/tmp/original_data/galaxytag.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert zoospec from '/tmp/original_data/zoospec.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert photoz from '/tmp/original_data/photoz.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert apogeestar from '/tmp/original_data/apogeestar.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galaxy from '/tmp/original_data/galaxy.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galspecextra from '/tmp/original_data/galspecextra.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galspecindx from '/tmp/original_data/galspecindx.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galspecline from '/tmp/original_data/galspecline.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert stellarmassfspsgranearlydust from '/tmp/original_data/stellarmassfspsgranearlydust.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert mangagalaxyzoo from '/tmp/original_data/mangagalaxyzoo.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert mangadrpall from '/tmp/original_data/mangadrpall.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert mangapipe3d from '/tmp/original_data/mangapipe3d.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)



