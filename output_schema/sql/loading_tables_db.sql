/* Login per db cli
Microsoft SQL Server: mssql -u sa -p ... -d db_benchmark -T 60 000
MySQL: mysql -u root -p... db_benchmark
PostgreSQL: psql -d db_benchmark -U eddy */

-- Code for importing all csv files per table in PostgreSQL commandline.
\copy public.photoobjall from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/photoobjall.csv' csv header delimiter ','
\copy public.photoobj from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/photoobj.csv' csv header delimiter ','
\copy public.specphotoall from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/specphotoall.csv' csv header delimiter ','
\copy public.specphoto from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/specphoto.csv' csv header delimiter ','
\copy public.spplines from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/spplines.csv' csv header delimiter ','
\copy public.sppparams from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/sppparams.csv' csv header delimiter ','
\copy public.wise_xmatch from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/wise_xmatch.csv' csv header delimiter ','
\copy public.phototag from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/phototag.csv' csv header delimiter ','
\copy public.galaxytag from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galaxytag.csv' csv header delimiter ','
\copy public.zoospec from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/zoospec.csv' csv header delimiter ','
\copy public.photoz from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/photoz.csv' csv header delimiter ','
\copy public.apogeestar from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/apogeestar.csv' csv header delimiter ','
\copy public.galaxy from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galaxy.csv' csv header delimiter ','
\copy public.galspecextra from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galspecextra.csv' csv header delimiter ','
\copy public.galspecindx from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galspecindx.csv' csv header delimiter ','
\copy public.galspecline from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/galspecline.csv' csv header delimiter ','
\copy public.stellarmassfspsgranearlydust from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/stellarmassfspsgranearlydust.csv' csv header delimiter ','
\copy public.mangagalaxyzoo from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/mangagalaxyzoo.csv' csv header delimiter ','
\copy public.mangadrpall from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/mangadrpall.csv' csv header delimiter ','
\copy public.mangapipe3d from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables/mangapipe3d.csv' csv header delimiter ','

\copy public.photoobjall from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/photoobjall.csv' csv header delimiter ','
\copy public.photoobj from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/photoobj.csv' csv header delimiter ','
\copy public.specphotoall from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/specphotoall.csv' csv header delimiter ','
\copy public.specphoto from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/specphoto.csv' csv header delimiter ','
\copy public.spplines from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/spplines.csv' csv header delimiter ','
\copy public.sppparams from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/sppparams.csv' csv header delimiter ','
\copy public.wise_xmatch from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/wise_xmatch.csv' csv header delimiter ','
\copy public.phototag from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/phototag.csv' csv header delimiter ','
\copy public.galaxytag from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/galaxytag.csv' csv header delimiter ','
\copy public.zoospec from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/zoospec.csv' csv header delimiter ','
\copy public.photoz from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/photoz.csv' csv header delimiter ','
\copy public.apogeestar from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/apogeestar.csv' csv header delimiter ','
\copy public.galaxy from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/galaxy.csv' csv header delimiter ','
\copy public.galspecextra from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/galspecextra.csv' csv header delimiter ','
\copy public.galspecindx from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/galspecindx.csv' csv header delimiter ','
\copy public.galspecline from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/galspecline.csv' csv header delimiter ','
\copy public.stellarmassfspsgranearlydust from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/stellarmassfspsgranearlydust.csv' csv header delimiter ','
\copy public.mangagalaxyzoo from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/mangagalaxyzoo.csv' csv header delimiter ','
\copy public.mangadrpall from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/mangadrpall.csv' csv header delimiter ','
\copy public.mangapipe3d from '~/Documents/study_github/2IMC00_thesis_benchmark/output_tables_generated/mangapipe3d.csv' csv header delimiter ','

-- Code for importing all csv files per table in MySQL commandline. EXTRA Info: docker cp PATH_CSV_FILE CONTAINER_NAME:/
load data infile '/tmp/original_tables/photoobjall.csv' into table photoobjall fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/photoobj.csv' into table photoobj fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/specphotoall.csv' into table specphotoall fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/specphoto.csv' into table specphoto fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/spplines.csv' into table spplines fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/sppparams.csv' into table sppparams fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/wise_xmatch.csv' into table wise_xmatch fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/phototag.csv' into table phototag fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/galaxytag.csv' into table galaxytag fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/zoospec.csv' into table zoospec fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/photoz.csv' into table photoz fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/apogeestar.csv' into table apogeestar fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/galaxy.csv' into table galaxy fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/galspecextra.csv' into table galspecextra fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/galspecindx.csv' into table galspecindx fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/galspecline.csv' into table galspecline fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/stellarmassfspsgranearlydust.csv' into table stellarmassfspsgranearlydust fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/mangagalaxyzoo.csv' into table mangagalaxyzoo fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/mangadrpall.csv' into table mangadrpall fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/original_tables/mangapipe3d.csv' into table mangapipe3d fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;

load data infile '/tmp/generated_tables/photoobjall.csv' into table photoobjall fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/photoobj.csv' into table photoobj fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/specphotoall.csv' into table specphotoall fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/specphoto.csv' into table specphoto fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/spplines.csv' into table spplines fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/sppparams.csv' into table sppparams fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/wise_xmatch.csv' into table wise_xmatch fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/phototag.csv' into table phototag fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/galaxytag.csv' into table galaxytag fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/zoospec.csv' into table zoospec fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/photoz.csv' into table photoz fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/apogeestar.csv' into table apogeestar fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/galaxy.csv' into table galaxy fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/galspecextra.csv' into table galspecextra fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/galspecindx.csv' into table galspecindx fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/galspecline.csv' into table galspecline fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/stellarmassfspsgranearlydust.csv' into table stellarmassfspsgranearlydust fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/mangagalaxyzoo.csv' into table mangagalaxyzoo fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/mangadrpall.csv' into table mangadrpall fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;
load data infile '/tmp/generated_tables/mangapipe3d.csv' into table mangapipe3d fields terminated by ',' enclosed by '"' lines terminated by '\n' ignore 1 rows;

-- Code for importing all csv files per table in mssql.
/* When using SQL Sever with Docker do the following:
1. Create new folder in tmp
2. Copy the tables files to folder create above to the container using: docker cp PATH_CSV_FILES/. CONTAINER_NAME:/tmp/FOLDER_NAME_STEP1

Use 'docker exec -u 0 CONTAINER_NAME COMMAND_TO_EXECUTE' to execute bash commands such as removing files.
*/
bulk insert photoobjall from '/tmp/original_tables/photoobjall.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert photoobj from '/tmp/original_tables/photoobj.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert specphotoall from '/tmp/original_tables/specphotoall.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert specphoto from '/tmp/original_tables/specphoto.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert spplines from '/tmp/original_tables/spplines.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert sppparams from '/tmp/original_tables/sppparams.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert wise_xmatch from '/tmp/original_tables/wise_xmatch.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert phototag from '/tmp/original_tables/phototag.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galaxytag from '/tmp/original_tables/galaxytag.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert zoospec from '/tmp/original_tables/zoospec.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert photoz from '/tmp/original_tables/photoz.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert apogeestar from '/tmp/original_tables/apogeestar.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galaxy from '/tmp/original_tables/galaxy.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galspecextra from '/tmp/original_tables/galspecextra.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galspecindx from '/tmp/original_tables/galspecindx.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galspecline from '/tmp/original_tables/galspecline.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert stellarmassfspsgranearlydust from '/tmp/original_tables/stellarmassfspsgranearlydust.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert mangagalaxyzoo from '/tmp/original_tables/mangagalaxyzoo.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert mangadrpall from '/tmp/original_tables/mangadrpall.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert mangapipe3d from '/tmp/original_tables/mangapipe3d.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)

bulk insert photoobjall from '/tmp/generated_tables/photoobjall.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert photoobj from '/tmp/generated_tables/photoobj.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert specphotoall from '/tmp/generated_tables/specphotoall.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert specphoto from '/tmp/generated_tables/specphoto.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert spplines from '/tmp/generated_tables/spplines.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert sppparams from '/tmp/generated_tables/sppparams.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert wise_xmatch from '/tmp/generated_tables/wise_xmatch.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert phototag from '/tmp/generated_tables/phototag.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galaxytag from '/tmp/generated_tables/galaxytag.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert zoospec from '/tmp/generated_tables/zoospec.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert photoz from '/tmp/generated_tables/photoz.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert apogeestar from '/tmp/generated_tables/apogeestar.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galaxy from '/tmp/generated_tables/galaxy.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galspecextra from '/tmp/generated_tables/galspecextra.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galspecindx from '/tmp/generated_tables/galspecindx.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert galspecline from '/tmp/generated_tables/galspecline.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert stellarmassfspsgranearlydust from '/tmp/generated_tables/stellarmassfspsgranearlydust.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert mangagalaxyzoo from '/tmp/generated_tables/mangagalaxyzoo.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert mangadrpall from '/tmp/generated_tables/mangadrpall.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)
bulk insert mangapipe3d from '/tmp/generated_tables/mangapipe3d.csv' with (firstrow=2, fieldterminator=',', rowterminator='\n', tablock)





\copy public.photoobjall to '~/test/photoobjall.csv' csv header delimiter ','";
\copy public.photoobj to '~/test/photoobj.csv' csv header delimiter ',';
\copy public.specphotoall to '~/test/specphotoall.csv' csv header delimiter ',';
\copy public.specphoto to '~/test/specphoto.csv' csv header delimiter ',';
\copy public.spplines to '~/test/spplines.csv' csv header delimiter ',';
\copy public.sppparams to '~/test/sppparams.csv' csv header delimiter ',';
\copy public.wise_xmatch to '~/test/wise_xmatch.csv' csv header delimiter ',';
\copy public.phototag to '~/test/phototag.csv' csv header delimiter ',';
\copy public.galaxytag to '~/test/galaxytag.csv' csv header delimiter ',';
\copy public.zoospec to '~/test/zoospec.csv' csv header delimiter ',';
\copy public.photoz to '~/test/photoz.csv' csv header delimiter ',';
\copy public.apogeestar to '~/test/apogeestar.csv' csv header delimiter ',';
\copy public.galaxy to '~/test/galaxy.csv' csv header delimiter ',';
\copy public.galspecextra to '~/test/galspecextra.csv' csv header delimiter ',';
\copy public.galspecindx to '~/test/galspecindx.csv' csv header delimiter ',';
\copy public.galspecline to '~/test/galspecline.csv' csv header delimiter ',';
\copy public.stellarmassfspsgranearlydust to '~/test/stellarmassfspsgranearlydust.csv' csv header delimiter ',';
\copy public.mangagalaxyzoo to '~/test/mangagalaxyzoo.csv' csv header delimiter ',';
\copy public.mangadrpall to '~/test/mangadrpall.csv' csv header delimiter ',';
\copy public.mangapipe3d to '~/test/mangapipe3d.csv' csv header delimiter ',';


