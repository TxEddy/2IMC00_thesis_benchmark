--------------------------
-- Queries for the logs. -
--------------------------

-- Retrieve distinct dbname and access columns of SkyServer.
SELECT distinct dbname, access
FROM SqlLog
WHERE yy = 2020
ORDER BY dbname

-- or
SELECT distinct dbname, access
FROM SqlLog
WHERE yy = 2020 and dbname = 'BestDR16'
ORDER BY dbname

-- Base for retrieving user query statements
SELECT top 1000 *
FROM SqlLog
WHERE yy = 2020
    and mm = 12
    and dbname = 'BestDR16'
    /*and access like 'Skyserver.Search%'*/
    and access like 'SkyserverWS.%'
    and (rows > 0 and error != 1)


-- Advanced for retrieving user query statements
SELECT top 1000 *
FROM SqlLog
WHERE yy = 2020
    and mm = 12
    and dbname = 'BestDR16'
    and (access like 'Skyserver.Search%' or access like 'SkyserverWS.%')
    and (rows > 0 and error != 1)
    and lower(statement) like '%join%'

-- Advanced for retrieving user query statements
SELECT top 1000 *
FROM SqlLog
WHERE yy = 2020
    and mm = 12
    and dbname = 'BestDR16'
    and (access like 'Skyserver.Search%' or access like 'SkyserverWS.%')
    and (rows > 0 and error != 1)
    and (lower(statement) like 'select%' or lower(statement) like '%count(%' or lower(statement)  like '%avg(%' or lower(statement)  like '%sum(%' or lower(statement) like '%join%')


-- Advanced for retrieving user query statements.
-- Query fixed by Hamid.
-- Moving the "and lower(statement) like '%join%'" inside aggregation statement and changing and into or yields in more results.
SELECT top 1100 *
FROM SqlLog
WHERE yy = 2020
    and mm = 10
    and dbname = 'BestDR16'
    and (access like 'Skyserver.Search%')
    and (rows > 0 and error != 1)
    and (lower(statement) like '%count(%'
    or lower(statement) like '%avg(%'
    or lower(statement) like '%sum(%'
    )
    and lower(statement) like '%join%'


-- Advanced for retrieving user query statements having top20 tables.
-- Query fixed by Hamid
-- Moving the "and lower(statement) like '%join%'" inside aggregation statement and changing and into or yields in more results.
SELECT top 1100 *
FROM SqlLog
WHERE yy = 2020
    and mm = 10
    and dbname = 'BestDR16'
    and (access like 'Skyserver.Search%')
    and (rows > 0 and error != 1)
    and (lower(statement) like '%count(%'
    or lower(statement) like '%avg(%'
    or lower(statement) like '%sum(%'
    )
    and lower(statement) like '%join%'
    and (lower(statement) like '%phototag%'
    or lower(statement) like '%sdssebossfirefly%'
    or lower(statement) like '%photoobj%'
    or lower(statement) like '%galaxy%'
    or lower(statement) like '%photoprimary%'
    or lower(statement) like '%specphoto%'
    or lower(statement) like '%star%'
    or lower(statement) like '%galaxytag%'
    or lower(statement) like '%photoobjall%'
    or lower(statement) like '%specphotoall%'
    or lower(statement) like '%galspecline%'
    or lower(statement) like '%first%'
    or lower(statement) like '%emissionlinesport%'
    or lower(statement) like '%photoz%'
    or lower(statement) like '%spplines%'
    or lower(statement) like '%field%'
    or lower(statement) like '%apogeestar%'
    or lower(statement) like '%wise_xmatch%'
    or lower(statement) like '%sppparams%'
    or lower(statement) like '%stellarmassstarformingport%'
    )
    and (lower(statement) not like '%fget%'
    )


-- Advanced for retrieving user query statements having top20 tables.
-- Specifically not including the tables specobj and specobjall and function tables like 'fget'.
-- Query fixed by Hamid
-- Moving the "and lower(statement) like '%join%'" inside aggregation statement and changing and into or yields in more results.
SELECT top 1100 *
FROM SqlLog
WHERE yy = 2020
    and mm = 10
    and dbname = 'BestDR16'
    and (access like 'Skyserver.Search%')
    and (rows > 0 and error != 1)
    and (lower(statement) like '%count(%'
    or lower(statement) like '%avg(%'
    or lower(statement) like '%sum(%'
    or lower(statement) like '%group by%'
    or lower(statement) like '%having%'
    )
    and lower(statement) like '%join%'
    and (lower(statement) like '%phototag%'
    or lower(statement) like '%sdssebossfirefly%'
    or lower(statement) like '%photoobj%'
    or lower(statement) like '%galaxy%'
    or lower(statement) like '%photoprimary%'
    or lower(statement) like '%specphoto%'
    or lower(statement) like '%star%'
    or lower(statement) like '%galaxytag%'
    or lower(statement) like '%photoobjall%'
    or lower(statement) like '%specphotoall%'
    or lower(statement) like '%galspecline%'
    or lower(statement) like '%first%'
    or lower(statement) like '%emissionlinesport%'
    or lower(statement) like '%photoz%'
    or lower(statement) like '%spplines%'
    or lower(statement) like '%field%'
    or lower(statement) like '%apogeestar%'
    or lower(statement) like '%wise_xmatch%'
    or lower(statement) like '%sppparams%'
    or lower(statement) like '%stellarmassstarformingport%'
    )
    and lower(statement) not like '%specob%'
    and lower(statement) not like '%fget%'


SELECT top 1100 * FROM SqlLog WHERE yy = 2020 and mm = 12 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%'

SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and mm = 11 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%' or lower(statement) like '%join%')

-- Advanced for retrieving user query statements having top20 tables.
SELECT top 1100 * FROM SqlLog WHERE yy = 2020 and mm = 10 and dbname = 'BestDR16' and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and (lower(statement) not like '%fget%')

--ADVANCED FOR RETRIEVING AND EXCLUDE SPECIFIC TABLES.
SELECT top 100000 * FROM SqlLog WHERE yy = 2020 and dbname = 'BestDR16' and (access like 'Skyserver.Search%')and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%') and lower(statement) like '%join%' and (lower(statement) like '%phototag%' or lower(statement) like '%sdssebossfirefly%' or lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%photoprimary%' or lower(statement) like '%specphoto%' or lower(statement) like '%star%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoobjall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%galspecline%' or lower(statement) like '%first%' or lower(statement) like '%emissionlinesport%' or lower(statement) like '%photoz%' or lower(statement) like '%spplines%' or lower(statement) like '%field%' or lower(statement) like '%apogeestar%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%sppparams%' or lower(statement) like '%stellarmassstarformingport%') and lower(statement) not like '%specob%' and lower(statement) not like '%fget%'


-------------------------------------------------------------------
--                    Queries for the logs.                      --
-------------------------------------------------------------------
-- * New method, download for the Year 2020 (Only aggregation)   --
-- * Based on query log of the year 2020, create frequency table --
--   from which the Top 20 are selected.                         --
-- * From the Top 20 download log over same year (2020) which    --
--   include join, aggregation and select statements.            --
--   But not the 'fget'-tables, Specobj tables can be included,  --
--   However add these as empty tables.                          --
-------------------------------------------------------------------

-- Advanced for retrieving user query statements of the year 2020.
-- Should contain 2673 rows.
SELECT count(*)
FROM SqlLog
WHERE yy = 2020
    and access like 'Skyserver.Search%'
    and (rows > 0 and error != 1)
    and (lower(statement) like '%count(%'
    or lower(statement) like '%avg(%'
    or lower(statement) like '%sum(%'
    or lower(statement) like '%group by%'
    )
    and lower(statement) like '%join%'

-- String version:
SELECT * FROM SqlLog WHERE yy = 2020 and access like 'Skyserver.Search%' and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%' or lower(statement) like '%group by%') and lower(statement) like '%join%'


-- Advanced for retrieving user query statements having top20 tables.
-- Specifically not including tables like 'fget'.
-- Should contain 1770 rows, without "or lower(statement) like 'select%'".
-- Should contain 25543 rows, with "or lower(statement) like 'select%'".
SELECT count(*)
FROM SqlLog
WHERE yy = 2020
    and (access like 'Skyserver.Search%')
    and (rows > 0 and error != 1)
    and (lower(statement) like '%count(%'
    or lower(statement) like '%avg(%'
    or lower(statement) like '%sum(%'
    or lower(statement) like '%group by%'
    )
    and lower(statement) like '%join%'
    and (lower(statement) like '%photoobj%'
    or lower(statement) like '%galaxy%'
    or lower(statement) like '%specphoto%'
    or lower(statement) like '%stellarmassfspsgranearlydust%'
    or lower(statement) like '%photoobjall%'
    or lower(statement) like '%mangapipe3d%'
    or lower(statement) like '%mangadrpall%'
    or lower(statement) like '%specphotoall%'
    or lower(statement) like '%spplines%'
    or lower(statement) like '%galaxytag%'
    or lower(statement) like '%photoz%'
    or lower(statement) like '%sppparams%'
    or lower(statement) like '%galspecline%'
    or lower(statement) like '%galspecindx%'
    or lower(statement) like '%zoospec%'
    or lower(statement) like '%apogeestar%'
    or lower(statement) like '%phototag%'
    or lower(statement) like '%galspecextra%'
    or lower(statement) like '%wise_xmatch%'
    or lower(statement) like '%mangagalaxyzoo%'
    )
    and lower(statement) not like '%fget%'

-- String version:
SELECT * FROM SqlLog WHERE yy = 2020 and (access like 'Skyserver.Search%') and (rows > 0 and error != 1) and (lower(statement) like '%count(%' or lower(statement) like '%avg(%' or lower(statement) like '%sum(%' or lower(statement) like '%group by%') and lower(statement) like '%join%' and (lower(statement) like '%photoobj%' or lower(statement) like '%galaxy%' or lower(statement) like '%specphoto%' or lower(statement) like '%stellarmassfspsgranearlydust%' or lower(statement) like '%photoobjall%' or lower(statement) like '%mangapipe3d%' or lower(statement) like '%mangadrpall%' or lower(statement) like '%specphotoall%' or lower(statement) like '%spplines%' or lower(statement) like '%galaxytag%' or lower(statement) like '%photoz%' or lower(statement) like '%sppparams%' or lower(statement) like '%galspecline%' or lower(statement) like '%galspecindx%' or lower(statement) like '%zoospec%' or lower(statement) like '%apogeestar%' or lower(statement) like '%phototag%' or lower(statement) like '%galspecextra%' or lower(statement) like '%wise_xmatch%' or lower(statement) like '%mangagalaxyzoo%') and lower(statement) not like '%fget%'




---------------------------------------
-- Queries for the different tables. --
---------------------------------------
-- PhotoObjAll, count(distinct poa.objid) => 647562
-- SELECT distinct top 50000 poa.objid
SELECT distinct top 50000 poa.*
FROM photoobjall poa
JOIN photoobj po on po.objid = poa.objid
JOIN galaxy g on g.objid = poa.objid
JOIN specphoto sp on sp.objid = poa.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid
JOIN specphotoall spa on spa.objid = poa.objid
JOIN spplines sl on sl.bestobjid = poa.objid
JOIN galaxytag gt on gt.objid = poa.objid
JOIN photoz pz on pz.objid = poa.objid
JOIN sppparams sps on sps.bestobjid = poa.objid
JOIN galspecline gs on gs.specobjid = poa.specobjid
JOIN galspecindx gsi on gsi.specobjid = poa.specobjid
JOIN zoospec zs on zs.objid = poa.objid
JOIN phototag pt on pt.objid = poa.objid
JOIN galspecextra gse on gse.specobjid = poa.specobjid
JOIN wise_xmatch w on w.sdss_objid = poa.objid
ORDER BY poa.objid desc
-- range: WHERE poa.objid between 1237668333104136344 and 1237680262909460580

-- PhotoObj, count(po.objid) => 890553
SELECT distinct top 15000 po.*
FROM photoobj po
JOIN photoobjall poa on poa.objid = po.objid
JOIN galaxy g on g.objid = po.objid
JOIN specphoto sp on sp.objid = po.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = po.specobjid
JOIN specphotoall spa on spa.specobjid = po.specobjid
JOIN spplines sl on sl.bestobjid = po.objid
JOIN galaxytag gt on gt.objid = po.objid
JOIN photoz pz on pz.objid = po.objid
JOIN sppparams sps on sps.bestobjid = po.objid
JOIN galspecline gs on gs.specobjid = po.specobjid
JOIN galspecindx gsi on gsi.specobjid = po.specobjid
JOIN zoospec zs on zs.objid = po.objid
JOIN phototag pt on pt.objid = po.objid
JOIN galspecextra gse on gse.specobjid = po.specobjid
JOIN wise_xmatch w on w.sdss_objid = po.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- Galaxy, count(g.objid) => 1532674
SELECT distinct top 15000 g.*
FROM galaxy g
JOIN photoobjall poa on poa.objid = g.objid
JOIN photoobj po on po.objid = g.objid
JOIN specphoto sp on sp.objid = g.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = g.specobjid
JOIN specphotoall spa on spa.objid = g.objid
JOIN spplines sl on sl.bestobjid = g.objid
JOIN galaxytag gt on gt.objid = g.objid
JOIN photoz pz on pz.objid = g.objid
JOIN sppparams sps on sps.bestobjid = g.objid
JOIN galspecline gs on gs.specobjid = g.specobjid
JOIN galspecindx gsi on gsi.specobjid = g.specobjid
JOIN zoospec zs on zs.objid = g.objid
JOIN phototag pt on pt.objid = g.objid
JOIN galspecextra gse on gse.specobjid = g.specobjid
JOIN wise_xmatch w on w.sdss_objid = g.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- SpecPhoto, count(sp.objid) => 1545228
SELECT distinct top 15000 sp.*
FROM specphoto sp
JOIN photoobjall poa on poa.objid = sp.objid
JOIN galaxy g on g.objid = sp.objid
JOIN photoobj po on po.objid = sp.objid
JOIN specphotoall spa on spa.objid = sp.objid
JOIN spplines sl on sl.bestobjid = sp.objid
JOIN galaxytag gt on gt.objid = sp.objid
JOIN sppparams sps on sps.bestobjid = sp.objid
JOIN galspecline gs on gs.specobjid = sp.specobjid
JOIN galspecindx gsi on gsi.specobjid = sp.specobjid
JOIN zoospec zs on zs.objid = sp.objid
JOIN phototag pt on pt.objid = sp.objid
JOIN galspecextra gse on gse.specobjid = sp.specobjid
JOIN wise_xmatch w on w.sdss_objid = sp.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- stellarMassFSPSGranEarlyDust, count(smfged.specobjid) => 618944
SELECT distinct top 15000 smfged.*
FROM stellarmassfspsgranearlydust smfged
JOIN photoobjall poa on poa.specobjid = smfged.specobjid
JOIN galaxy g on g.specobjid = smfged.specobjid
JOIN specphoto sp on sp.specobjid = smfged.specobjid
JOIN specphotoall spa on spa.specobjid = smfged.specobjid
JOIN spplines sl on sl.specobjid = smfged.specobjid
JOIN galaxytag gt on gt.specobjid = smfged.specobjid
JOIN sppparams sps on sps.specobjid = smfged.specobjid
JOIN galspecline gs on gs.specobjid = smfged.specobjid
JOIN galspecindx gsi on gsi.specobjid = smfged.specobjid
JOIN zoospec zs on zs.specobjid = smfged.specobjid
JOIN phototag pt on pt.specobjid = smfged.specobjid
JOIN galspecextra gse on gse.specobjid = smfged.specobjid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- mangaPipe3D
SELECT top 15000 *
FROM mangapipe3d


-- mangaDRPall
SELECT top 15000 *
FROM mangadrpall
ORDER BY mangaid

-- SpecPhotoAll, count(spa.objid) => 1282729
SELECT distinct top 15000 spa.*
FROM specphotoall spa
JOIN photoobjall poa on poa.objid = spa.objid
JOIN photoobj po on po.objid = spa.objid
JOIN galaxy g on g.objid = spa.objid
JOIN specphoto sp on sp.objid = spa.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = spa.specobjid
JOIN spplines sl on sl.bestobjid = spa.objid
JOIN galaxytag gt on gt.objid = spa.objid
JOIN photoz pz on pz.objid = spa.objid
JOIN sppparams sps on sps.bestobjid = spa.objid
JOIN galspecline gs on gs.specobjid = spa.specobjid
JOIN galspecindx gsi on gsi.specobjid = spa.specobjid
JOIN zoospec zs on zs.objid = spa.objid
JOIN phototag pt on pt.objid = spa.objid
JOIN galspecextra gse on gse.specobjid = spa.specobjid
JOIN wise_xmatch w on w.sdss_objid = spa.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- sppLines, count(sl.bestobjid) => 1292323
SELECT distinct top 15000 sl.*
FROM spplines sl
JOIN photoobjall poa on poa.objid = sl.bestobjid
JOIN photoobj po on po.objid = sl.bestobjid
JOIN galaxy g on g.objid = sl.bestobjid
JOIN specphoto sp on sp.objid = sl.bestobjid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = sl.specobjid
JOIN specphotoall spa on spa.objid = sl.bestobjid
JOIN galaxytag gt on gt.objid = sl.bestobjid
JOIN photoz pz on pz.objid = sl.bestobjid
JOIN sppparams sps on sps.bestobjid = sl.bestobjid
JOIN galspecline gs on gs.specobjid = sl.specobjid
JOIN galspecindx gsi on gsi.specobjid = sl.specobjid
JOIN zoospec zs on zs.objid = sl.bestobjid
JOIN phototag pt on pt.objid = sl.bestobjid
JOIN galspecextra gse on gse.specobjid = sl.specobjid
JOIN wise_xmatch w on w.sdss_objid = sl.bestobjid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- GalaxyTag, count(gt.objid) => 1532674
SELECT distinct top 15000 gt.*
FROM galaxytag gt
JOIN photoobjall poa on poa.objid = gt.objid
JOIN photoobj po on po.objid = gt.objid
JOIN galaxy g on g.objid = gt.objid
JOIN specphoto sp on sp.objid = gt.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gt.specobjid
JOIN specphotoall spa on spa.objid = gt.objid
JOIN spplines sl on sl.bestobjid = gt.objid
JOIN photoz pz on pz.objid = gt.objid
JOIN sppparams sps on sps.bestobjid = gt.objid
JOIN zoospec zs on zs.objid = gt.objid
JOIN phototag pt on pt.objid = gt.objid
JOIN galspecextra gse on gse.specobjid = gt.specobjid
JOIN wise_xmatch w on w.sdss_objid = gt.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- Photoz, count(pz.objid) => 1532674
SELECT distinct top 15000 pz.*
FROM photoz pz
JOIN photoobjall poa on poa.objid = pz.objid
JOIN photoobj po on po.objid = pz.objid
JOIN galaxy g on g.objid = pz.objid
JOIN specphoto sp on sp.objid = pz.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid
JOIN specphotoall spa on spa.objid = pz.objid
JOIN spplines sl on sl.bestobjid = pz.objid
JOIN galaxytag gt on gt.objid = pz.objid
JOIN sppparams sps on sps.bestobjid = pz.objid
JOIN galspecline gs on gs.specobjid = poa.specobjid
JOIN galspecindx gsi on gsi.specobjid = poa.specobjid
JOIN zoospec zs on zs.objid = poa.objid
JOIN phototag pt on pt.objid = pz.objid
JOIN galspecextra gse on gse.specobjid = poa.specobjid
JOIN wise_xmatch w on w.sdss_objid = pz.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- sppParams, count(sps.bestobjid) => 890568
SELECT distinct top 15000 sps.*
FROM sppparams sps
JOIN photoobjall poa on poa.objid = sps.bestobjid
JOIN photoobj po on po.objid = sps.bestobjid
JOIN galaxy g on g.objid = sps.bestobjid
JOIN specphoto sp on sp.objid = sps.bestobjid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = sps.specobjid
JOIN specphotoall spa on spa.objid = sps.bestobjid
JOIN spplines sl on sl.bestobjid = sps.bestobjid
JOIN galaxytag gt on gt.objid = sps.bestobjid
JOIN photoz pz on pz.objid = sps.bestobjid
JOIN galspecline gs on gs.specobjid = sps.specobjid
JOIN galspecindx gsi on gsi.specobjid = sps.specobjid
JOIN zoospec zs on zs.objid = sps.bestobjid
JOIN phototag pt on pt.specobjid = sps.specobjid
JOIN galspecextra gse on gse.specobjid = sps.specobjid
JOIN wise_xmatch w on w.sdss_objid = sps.bestobjid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- galSpecLine, count(gs.specobjid) => 629464
SELECT distinct top 15000 gs.*
FROM galspecline gs
JOIN photoobjall poa on poa.specobjid = gs.specobjid
JOIN photoobj po on po.specobjid = gs.specobjid
JOIN galaxy g on g.specobjid = gs.specobjid
JOIN specphoto sp on sp.specobjid = gs.specobjid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gs.specobjid
JOIN specphotoall spa on spa.specobjid = gs.specobjid
JOIN spplines sl on sl.specobjid = gs.specobjid
JOIN galaxytag gt on gt.specobjid = gs.specobjid
JOIN photoz pz on pz.objid = poa.objid
JOIN sppparams sps on sps.specobjid = gs.specobjid
JOIN galspecindx gsi on gsi.specobjid = gs.specobjid
JOIN zoospec zs on zs.specobjid = gs.specobjid
JOIN phototag pt on pt.specobjid = gs.specobjid
JOIN galspecextra gse on gse.specobjid = gs.specobjid
JOIN wise_xmatch w on w.sdss_objid = poa.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- galSpecIndx, count(gsi.specobjid) => 629464
SELECT distinct top 15000 gsi.*
FROM galspecindx gsi
JOIN photoobjall poa on poa.specobjid = gsi.specobjid
JOIN galaxy g on g.specobjid = gsi.specobjid
JOIN specphoto sp on sp.specobjid = gsi.specobjid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gsi.specobjid
JOIN specphotoall spa on spa.specobjid = gsi.specobjid
JOIN spplines sl on sl.specobjid = gsi.specobjid
JOIN galaxytag gt on gt.specobjid = gsi.specobjid
JOIN photoz pz on pz.objid = poa.objid
JOIN sppparams sps on sps.specobjid = gsi.specobjid
JOIN galspecline gs on gs.specobjid = gsi.specobjid
JOIN zoospec zs on zs.specobjid = gsi.specobjid
JOIN phototag pt on pt.specobjid = gsi.specobjid
JOIN galspecextra gse on gse.specobjid = gsi.specobjid
JOIN wise_xmatch w on w.sdss_objid = poa.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- zooSpec, count(zs.objid) => 1024875
SELECT distinct top 15000 zs.*
FROM zoospec zs
JOIN photoobjall poa on poa.objid = zs.objid
JOIN photoobj po on po.objid = zs.objid
JOIN galaxy g on g.objid = zs.objid
JOIN specphoto sp on sp.objid = zs.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = zs.specobjid
JOIN specphotoall spa on spa.objid = zs.objid
JOIN spplines sl on sl.bestobjid = zs.objid
JOIN galaxytag gt on gt.objid = zs.objid
JOIN photoz pz on pz.objid = zs.objid
JOIN sppparams sps on sps.bestobjid = zs.objid
JOIN galspecline gs on gs.specobjid = zs.specobjid
JOIN galspecindx gsi on gsi.specobjid = zs.specobjid
JOIN phototag pt on pt.objid = zs.objid
JOIN galspecextra gse on gse.specobjid = zs.specobjid
JOIN wise_xmatch w on w.sdss_objid = zs.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- apogeeStar, count(asr.htmid) => 27331
SELECT distinct top 15000 asr.*
FROM apogeestar asr
JOIN photoobjall poa on poa.htmid = asr.htmid
JOIN photoobj po on po.htmid = asr.htmid
JOIN galaxy g on g.htmid = asr.htmid
JOIN specphoto sp on sp.htmid = asr.htmid
JOIN specphotoall spa on spa.htmid = asr.htmid
JOIN galaxytag gt on gt.htmid = asr.htmid
JOIN phototag pt on pt.htmid = asr.htmid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- PhotoTag, count(pt.objid) => 1532674
SELECT distinct top 15000 pt.*
FROM phototag pt
JOIN photoobjall poa on poa.objid = pt.objid
JOIN photoobj po on po.objid = pt.objid
JOIN galaxy g on g.objid = pt.objid
JOIN specphoto sp on sp.objid = pt.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = pt.specobjid
JOIN specphotoall spa on spa.objid = pt.objid
JOIN spplines sl on sl.bestobjid = pt.objid
JOIN galaxytag gt on gt.objid = pt.objid
JOIN photoz pz on pz.objid = pt.objid
JOIN sppparams sps on sps.bestobjid = pt.objid
JOIN galspecline gs on gs.specobjid = pt.specobjid
JOIN galspecindx gsi on gsi.specobjid = pt.specobjid
JOIN zoospec zs on zs.objid = pt.objid
JOIN galspecextra gse on gse.specobjid = pt.specobjid
JOIN wise_xmatch w on w.sdss_objid = pt.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- galSpecExtra, count(gse.specobjid) => 629464
SELECT distinct top 15000 gse.*
FROM galspecextra gse
JOIN photoobjall poa on poa.specobjid = gse.specobjid
JOIN photoobj po on po.specobjid = gse.specobjid
JOIN galaxy g on g.specobjid = gse.specobjid
JOIN specphoto sp on sp.specobjid = gse.specobjid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gse.specobjid
JOIN specphotoall spa on spa.specobjid = gse.specobjid
JOIN spplines sl on sl.specobjid = gse.specobjid
JOIN galaxytag gt on gt.specobjid = gse.specobjid
JOIN photoz pz on pz.objid = poa.objid
JOIN sppparams sps on sps.specobjid = gse.specobjid
JOIN galspecline gs on gs.specobjid = gse.specobjid
JOIN galspecindx gsi on gsi.specobjid = gse.specobjid
JOIN zoospec zs on zs.specobjid = gse.specobjid
JOIN phototag pt on pt.specobjid = gse.specobjid
JOIN wise_xmatch w on w.sdss_objid = poa.objid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- WISE_xmatch, count(w.sdss_objid) => 1532674
SELECT distinct top 15000 w.*
FROM wise_xmatch w
JOIN photoobjall poa on poa.objid = w.sdss_objid
JOIN photoobj po on po.objid = w.sdss_objid
JOIN galaxy g on g.objid = w.sdss_objid
JOIN specphoto sp on sp.objid = w.sdss_objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid
JOIN specphotoall spa on spa.objid = w.sdss_objid
JOIN spplines sl on sl.bestobjid = w.sdss_objid
JOIN galaxytag gt on gt.objid = w.sdss_objid
JOIN photoz pz on pz.objid = w.sdss_objid
JOIN sppparams sps on sps.bestobjid = w.sdss_objid
JOIN galspecline gs on gs.specobjid = poa.specobjid
JOIN galspecindx gsi on gsi.specobjid = poa.specobjid
JOIN zoospec zs on zs.objid = w.sdss_objid
JOIN phototag pt on pt.objid = w.sdss_objid
JOIN galspecextra gse on gse.specobjid = poa.specobjid
WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- mangaGalaxyZoo, count(mgz.nsa_id) => 4286
SELECT distinct top 15000 mgz.*
FROM mangagalaxyzoo mgz
JOIN mangadrpall mda on mda.nsa_nsaid = mgz.nsa_id
WHERE mda.mangaid between ... and ...


----------------------
-- Test QCS tables. --
----------------------
-- PhotoObjAll.
SELECT top 1100 p.*
FROM photoobjall p
JOIN specobjall s on s.bestobjid = p.objid

-- SpecObjAll.
SELECT top 1100 s.*
FROM specobjall s
JOIN photoobjall p on p.objid = s.bestobjid


---------------------------------------
-- Queries for the different tables. --
---------------------------------------
--          String Versions          --
---------------------------------------
-- PhotoObjAll
SELECT distinct top 15000 poa.* FROM photoobjall poa JOIN photoobj po on po.objid = poa.objid JOIN galaxy g on g.objid = poa.objid JOIN specphoto sp on sp.objid = poa.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid JOIN specphotoall spa on spa.objid = poa.objid JOIN spplines sl on sl.bestobjid = poa.objid JOIN galaxytag gt on gt.objid = poa.objid JOIN photoz pz on pz.objid = poa.objid JOIN sppparams sps on sps.bestobjid = poa.objid JOIN galspecline gs on gs.specobjid = poa.specobjid JOIN galspecindx gsi on gsi.specobjid = poa.specobjid JOIN zoospec zs on zs.objid = poa.objid JOIN phototag pt on pt.objid = poa.objid JOIN galspecextra gse on gse.specobjid = poa.specobjid JOIN wise_xmatch w on w.sdss_objid = poa.objid ORDER BY poa.objid asc

-- PhotoObj, count(po.objid) => 890553
SELECT distinct top 15000 po.* FROM photoobj po JOIN photoobjall poa on poa.objid = po.objid JOIN galaxy g on g.objid = po.objid JOIN specphoto sp on sp.objid = po.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = po.specobjid JOIN specphotoall spa on spa.specobjid = po.specobjid JOIN spplines sl on sl.bestobjid = po.objid JOIN galaxytag gt on gt.objid = po.objid JOIN photoz pz on pz.objid = po.objid JOIN sppparams sps on sps.bestobjid = po.objid JOIN galspecline gs on gs.specobjid = po.specobjid JOIN galspecindx gsi on gsi.specobjid = po.specobjid JOIN zoospec zs on zs.objid = po.objid JOIN phototag pt on pt.objid = po.objid JOIN galspecextra gse on gse.specobjid = po.specobjid JOIN wise_xmatch w on w.sdss_objid = po.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- Galaxy, count(g.objid) => 1532674
SELECT distinct top 15000 g.* FROM galaxy g JOIN photoobjall poa on poa.objid = g.objid JOIN photoobj po on po.objid = g.objid JOIN specphoto sp on sp.objid = g.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = g.specobjid JOIN specphotoall spa on spa.objid = g.objid JOIN spplines sl on sl.bestobjid = g.objid JOIN galaxytag gt on gt.objid = g.objid JOIN photoz pz on pz.objid = g.objid JOIN sppparams sps on sps.bestobjid = g.objid JOIN galspecline gs on gs.specobjid = g.specobjid JOIN galspecindx gsi on gsi.specobjid = g.specobjid JOIN zoospec zs on zs.objid = g.objid JOIN phototag pt on pt.objid = g.objid JOIN galspecextra gse on gse.specobjid = g.specobjid JOIN wise_xmatch w on w.sdss_objid = g.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- SpecPhoto, count(sp.objid) => 1545228
SELECT distinct top 15000 sp.* FROM specphoto sp JOIN photoobjall poa on poa.objid = sp.objid JOIN galaxy g on g.objid = sp.objid JOIN photoobj po on po.objid = sp.objid JOIN specphotoall spa on spa.objid = sp.objid JOIN spplines sl on sl.bestobjid = sp.objid JOIN galaxytag gt on gt.objid = sp.objid JOIN sppparams sps on sps.bestobjid = sp.objid JOIN galspecline gs on gs.specobjid = sp.specobjid JOIN galspecindx gsi on gsi.specobjid = sp.specobjid JOIN zoospec zs on zs.objid = sp.objid JOIN phototag pt on pt.objid = sp.objid JOIN galspecextra gse on gse.specobjid = sp.specobjid JOIN wise_xmatch w on w.sdss_objid = sp.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- stellarMassFSPSGranEarlyDust, count(smfged.specobjid) => 618944
SELECT distinct top 15000 smfged.* FROM stellarmassfspsgranearlydust smfged JOIN photoobjall poa on poa.specobjid = smfged.specobjid JOIN galaxy g on g.specobjid = smfged.specobjid JOIN specphoto sp on sp.specobjid = smfged.specobjid JOIN specphotoall spa on spa.specobjid = smfged.specobjid JOIN spplines sl on sl.specobjid = smfged.specobjid JOIN galaxytag gt on gt.specobjid = smfged.specobjid JOIN sppparams sps on sps.specobjid = smfged.specobjid JOIN galspecline gs on gs.specobjid = smfged.specobjid JOIN galspecindx gsi on gsi.specobjid = smfged.specobjid JOIN zoospec zs on zs.specobjid = smfged.specobjid JOIN phototag pt on pt.specobjid = smfged.specobjid JOIN galspecextra gse on gse.specobjid = smfged.specobjid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- mangaPipe3D
SELECT distinct top 15000 * FROM mangapipe3d

-- mangaDRPall
SELECT distinct top 15000 * FROM mangadrpall ORDER BY mangaid

-- SpecPhotoAll, count(spa.objid) => 1282729
SELECT distinct top 15000 spa.* FROM specphotoall spa JOIN photoobjall poa on poa.objid = spa.objid JOIN photoobj po on po.objid = spa.objid JOIN galaxy g on g.objid = spa.objid JOIN specphoto sp on sp.objid = spa.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = spa.specobjid JOIN spplines sl on sl.bestobjid = spa.objid JOIN galaxytag gt on gt.objid = spa.objid JOIN photoz pz on pz.objid = spa.objid JOIN sppparams sps on sps.bestobjid = spa.objid JOIN galspecline gs on gs.specobjid = spa.specobjid JOIN galspecindx gsi on gsi.specobjid = spa.specobjid JOIN zoospec zs on zs.objid = spa.objid JOIN phototag pt on pt.objid = spa.objid JOIN galspecextra gse on gse.specobjid = spa.specobjid JOIN wise_xmatch w on w.sdss_objid = spa.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- sppLines, count(sl.bestobjid) => 1292323
SELECT distinct top 15000 sl.* FROM spplines sl JOIN photoobjall poa on poa.objid = sl.bestobjid JOIN photoobj po on po.objid = sl.bestobjid JOIN galaxy g on g.objid = sl.bestobjid JOIN specphoto sp on sp.objid = sl.bestobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = sl.specobjid JOIN specphotoall spa on spa.objid = sl.bestobjid JOIN galaxytag gt on gt.objid = sl.bestobjid JOIN photoz pz on pz.objid = sl.bestobjid JOIN sppparams sps on sps.bestobjid = sl.bestobjid JOIN galspecline gs on gs.specobjid = sl.specobjid JOIN galspecindx gsi on gsi.specobjid = sl.specobjid JOIN zoospec zs on zs.objid = sl.bestobjid JOIN phototag pt on pt.objid = sl.bestobjid JOIN galspecextra gse on gse.specobjid = sl.specobjid JOIN wise_xmatch w on w.sdss_objid = sl.bestobjid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- GalaxyTag, count(gt.objid) => 1532674
SELECT distinct top 15000 gt.* FROM galaxytag gt JOIN photoobjall poa on poa.objid = gt.objid JOIN photoobj po on po.objid = gt.objid JOIN galaxy g on g.objid = gt.objid JOIN specphoto sp on sp.objid = gt.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gt.specobjid JOIN specphotoall spa on spa.objid = gt.objid JOIN spplines sl on sl.bestobjid = gt.objid JOIN photoz pz on pz.objid = gt.objid JOIN sppparams sps on sps.bestobjid = gt.objid JOIN zoospec zs on zs.objid = gt.objid JOIN phototag pt on pt.objid = gt.objid JOIN galspecextra gse on gse.specobjid = gt.specobjid JOIN wise_xmatch w on w.sdss_objid = gt.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- Photoz, count(pz.objid) => 1532674
SELECT distinct top 15000 pz.* FROM photoz pz JOIN photoobjall poa on poa.objid = pz.objid JOIN photoobj po on po.objid = pz.objid JOIN galaxy g on g.objid = pz.objid JOIN specphoto sp on sp.objid = pz.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid JOIN specphotoall spa on spa.objid = pz.objid JOIN spplines sl on sl.bestobjid = pz.objid JOIN galaxytag gt on gt.objid = pz.objid JOIN sppparams sps on sps.bestobjid = pz.objid JOIN galspecline gs on gs.specobjid = poa.specobjid JOIN galspecindx gsi on gsi.specobjid = poa.specobjid JOIN zoospec zs on zs.objid = poa.objid JOIN phototag pt on pt.objid = pz.objid JOIN galspecextra gse on gse.specobjid = poa.specobjid JOIN wise_xmatch w on w.sdss_objid = pz.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- sppParams, count(sps.bestobjid) => 890568
SELECT distinct top 15000 sps.* FROM sppparams sps JOIN photoobjall poa on poa.objid = sps.bestobjid JOIN photoobj po on po.objid = sps.bestobjid JOIN galaxy g on g.objid = sps.bestobjid JOIN specphoto sp on sp.objid = sps.bestobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = sps.specobjid JOIN specphotoall spa on spa.objid = sps.bestobjid JOIN spplines sl on sl.bestobjid = sps.bestobjid JOIN galaxytag gt on gt.objid = sps.bestobjid JOIN photoz pz on pz.objid = sps.bestobjid JOIN galspecline gs on gs.specobjid = sps.specobjid JOIN galspecindx gsi on gsi.specobjid = sps.specobjid JOIN zoospec zs on zs.objid = sps.bestobjid JOIN phototag pt on pt.specobjid = sps.specobjid JOIN galspecextra gse on gse.specobjid = sps.specobjid JOIN wise_xmatch w on w.sdss_objid = sps.bestobjid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- galSpecLine, count(gs.specobjid) => 629464
SELECT distinct top 15000 gs.* FROM galspecline gs JOIN photoobjall poa on poa.specobjid = gs.specobjid JOIN photoobj po on po.specobjid = gs.specobjid JOIN galaxy g on g.specobjid = gs.specobjid JOIN specphoto sp on sp.specobjid = gs.specobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gs.specobjid JOIN specphotoall spa on spa.specobjid = gs.specobjid JOIN spplines sl on sl.specobjid = gs.specobjid JOIN galaxytag gt on gt.specobjid = gs.specobjid JOIN photoz pz on pz.objid = poa.objid JOIN sppparams sps on sps.specobjid = gs.specobjid JOIN galspecindx gsi on gsi.specobjid = gs.specobjid JOIN zoospec zs on zs.specobjid = gs.specobjid JOIN phototag pt on pt.specobjid = gs.specobjid JOIN galspecextra gse on gse.specobjid = gs.specobjid JOIN wise_xmatch w on w.sdss_objid = poa.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- galSpecIndx, count(gsi.specobjid) => 629464
SELECT distinct top 15000 gsi.* FROM galspecindx gsi JOIN photoobjall poa on poa.specobjid = gsi.specobjid JOIN galaxy g on g.specobjid = gsi.specobjid JOIN specphoto sp on sp.specobjid = gsi.specobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gsi.specobjid JOIN specphotoall spa on spa.specobjid = gsi.specobjid JOIN spplines sl on sl.specobjid = gsi.specobjid JOIN galaxytag gt on gt.specobjid = gsi.specobjid JOIN photoz pz on pz.objid = poa.objid JOIN sppparams sps on sps.specobjid = gsi.specobjid JOIN galspecline gs on gs.specobjid = gsi.specobjid JOIN zoospec zs on zs.specobjid = gsi.specobjid JOIN phototag pt on pt.specobjid = gsi.specobjid JOIN galspecextra gse on gse.specobjid = gsi.specobjid JOIN wise_xmatch w on w.sdss_objid = poa.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- zooSpec, count(zs.objid) => 1024875
SELECT distinct top 15000 zs.* FROM zoospec zs JOIN photoobjall poa on poa.objid = zs.objid JOIN photoobj po on po.objid = zs.objid JOIN galaxy g on g.objid = zs.objid JOIN specphoto sp on sp.objid = zs.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = zs.specobjid JOIN specphotoall spa on spa.objid = zs.objid JOIN spplines sl on sl.bestobjid = zs.objid JOIN galaxytag gt on gt.objid = zs.objid JOIN photoz pz on pz.objid = zs.objid JOIN sppparams sps on sps.bestobjid = zs.objid JOIN galspecline gs on gs.specobjid = zs.specobjid JOIN galspecindx gsi on gsi.specobjid = zs.specobjid JOIN phototag pt on pt.objid = zs.objid JOIN galspecextra gse on gse.specobjid = zs.specobjid JOIN wise_xmatch w on w.sdss_objid = zs.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- apogeeStar, count(asr.htmid) => 27331
SELECT distinct top 15000 asr.* FROM apogeestar asr JOIN photoobjall poa on poa.htmid = asr.htmid JOIN photoobj po on po.htmid = asr.htmid JOIN galaxy g on g.htmid = asr.htmid JOIN specphoto sp on sp.htmid = asr.htmid JOIN specphotoall spa on spa.htmid = asr.htmid JOIN galaxytag gt on gt.htmid = asr.htmid JOIN phototag pt on pt.htmid = asr.htmid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- PhotoTag, count(pt.objid) => 1532674
SELECT distinct top 15000 pt.* FROM phototag pt JOIN photoobjall poa on poa.objid = pt.objid JOIN photoobj po on po.objid = pt.objid JOIN galaxy g on g.objid = pt.objid JOIN specphoto sp on sp.objid = pt.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = pt.specobjid JOIN specphotoall spa on spa.objid = pt.objid JOIN spplines sl on sl.bestobjid = pt.objid JOIN galaxytag gt on gt.objid = pt.objid JOIN photoz pz on pz.objid = pt.objid JOIN sppparams sps on sps.bestobjid = pt.objid JOIN galspecline gs on gs.specobjid = pt.specobjid JOIN galspecindx gsi on gsi.specobjid = pt.specobjid JOIN zoospec zs on zs.objid = pt.objid JOIN galspecextra gse on gse.specobjid = pt.specobjid JOIN wise_xmatch w on w.sdss_objid = pt.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- galSpecExtra, count(gse.specobjid) => 629464
SELECT distinct top 15000 gse.* FROM galspecextra gse JOIN photoobjall poa on poa.specobjid = gse.specobjid JOIN photoobj po on po.specobjid = gse.specobjid JOIN galaxy g on g.specobjid = gse.specobjid JOIN specphoto sp on sp.specobjid = gse.specobjid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = gse.specobjid JOIN specphotoall spa on spa.specobjid = gse.specobjid JOIN spplines sl on sl.specobjid = gse.specobjid JOIN galaxytag gt on gt.specobjid = gse.specobjid JOIN photoz pz on pz.objid = poa.objid JOIN sppparams sps on sps.specobjid = gse.specobjid JOIN galspecline gs on gs.specobjid = gse.specobjid JOIN galspecindx gsi on gsi.specobjid = gse.specobjid JOIN zoospec zs on zs.specobjid = gse.specobjid JOIN phototag pt on pt.specobjid = gse.specobjid JOIN wise_xmatch w on w.sdss_objid = poa.objid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- WISE_xmatch, count(w.sdss_objid) => 1532674
SELECT distinct top 15000 w.* FROM wise_xmatch w JOIN photoobjall poa on poa.objid = w.sdss_objid JOIN photoobj po on po.objid = w.sdss_objid JOIN galaxy g on g.objid = w.sdss_objid JOIN specphoto sp on sp.objid = w.sdss_objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid JOIN specphotoall spa on spa.objid = w.sdss_objid JOIN spplines sl on sl.bestobjid = w.sdss_objid JOIN galaxytag gt on gt.objid = w.sdss_objid JOIN photoz pz on pz.objid = w.sdss_objid JOIN sppparams sps on sps.bestobjid = w.sdss_objid JOIN galspecline gs on gs.specobjid = poa.specobjid JOIN galspecindx gsi on gsi.specobjid = poa.specobjid JOIN zoospec zs on zs.objid = w.sdss_objid JOIN phototag pt on pt.objid = w.sdss_objid JOIN galspecextra gse on gse.specobjid = poa.specobjid WHERE poa.objid between 1237645941824356443 and 1237649921110442096

-- mangaGalaxyZoo, count(mgz.nsa_id) => 4286
SELECT distinct top 15000 mgz.* FROM mangagalaxyzoo mgz JOIN mangadrpall mda on mda.nsa_nsaid = mgz.nsa_id WHERE mda.mangaid between ... and ...




-----------------------------------------------
--     Query for the correlation matrix.     --
-----------------------------------------------
--          String Version Included          --
-----------------------------------------------
SELECT g.clean as galaxy_clean,
	g.dec as galaxy_dec,
	g.g as galaxy_g,
	g.petromag_r as galaxy_petromag_r,
	g.petromag_u as galaxy_petromag_u,
	g.petror90_g as galaxy_petror90_g,
	g.petror90_r as galaxy_petror90_r,
	g.petrorad_u as galaxy_petrorad_u,
	g.r as galaxy_r,
	g.ra as galaxy_ra,
	gt.dec as galaxytag_dec,
	gt.ra as galaxytag_ra,
	gt.type as galaxytag_type,
	gse.bptclass as galspecextra_bptclass,
	gse.sfr_fib_p50 as galspecextra_sfr_fib_p50,
	gse.sfr_tot_p50 as galspecextra_sfr_tot_p50,
	gse.sfr_tot_p84 as galspecextra_sfr_tot_p84,
	gse.specsfr_tot_p50 as galspecextra_specsfr_tot_p50,
	gsi.d4000_n as galspecindx_d4000_n,
	gs.h_alpha_eqw as galspecline_h_alpha_eqw,
	gs.h_alpha_flux as galspecline_h_alpha_flux,
	gs.h_alpha_flux_err as galspecline_h_alpha_flux_err,
	gs.h_beta_eqw as galspecline_h_beta_eqw,
	gs.h_beta_flux as galspecline_h_beta_flux,
	gs.h_beta_flux_err as galspecline_h_beta_flux_err,
	gs.nii_6584_flux as galspecline_nii_6584_flux,
	gs.oi_6300_flux_err as galspecline_oi_6300_flux_err,
	gs.oiii_5007_eqw as galspecline_oiii_5007_eqw,
	gs.oiii_5007_flux as galspecline_oiii_5007_flux,
	gs.sii_6717_flux as galspecline_sii_6717_flux,
	gs.sii_6731_flux_err as galspecline_sii_6731_flux_err,
	po.b as photoobj_b,
	po.camcol as photoobj_camcol,
	po.clean as photoobj_clean,
	po.cmodelmag_g as photoobj_cmodelmag_g,
	po.dec as photoobj_dec,
	po.devrad_g as photoobj_devrad_g,
	po.devrad_r as photoobj_devrad_r,
	po.fibermag_r as photoobj_fibermag_r,
	po.field as photoobj_field,
	po.flags as photoobj_flags,
	po.fracdev_r as photoobj_fracdev_r,
	po.g as photoobj_g,
	po.l as photoobj_l,
	po.mode as photoobj_mode,
	po.petromag_r as photoobj_petromag_r,
	po.petromag_z as photoobj_petromag_z,
	po.petror50_g as photoobj_petror50_g,
	po.petror50_r as photoobj_petror50_r,
	po.petrorad_g as photoobj_petrorad_g,
	po.petrorad_r as photoobj_petrorad_r,
	po.r as photoobj_r,
	po.ra as photoobj_ra,
	po.run as photoobj_run,
	po.type as photoobj_type,
	po.u as photoobj_u,
	poa.camcol as photoobjall_camcol,
	poa.clean as photoobjall_clean,
	poa.dec as photoobjall_dec,
	poa.dered_r as photoobjall_dered_r,
	poa.devrad_r as photoobjall_devrad_r,
	poa.devraderr_r as photoobjall_devraderr_r,
	poa.exprad_r as photoobjall_exprad_r,
	poa.field as photoobjall_field,
	poa.fracdev_r as photoobjall_fracdev_r,
	poa.mode as photoobjall_mode,
	poa.petromag_r as photoobjall_petromag_r,
	poa.ra as photoobjall_ra,
	poa.run as photoobjall_run,
	poa.type as photoobjall_type,
	poa.u as photoobjall_u,
	pt.clean as phototag_clean,
	pt.dec as phototag_dec,
	pt.mode as phototag_mode,
	pt.nchild as phototag_nchild,
	pt.psfmag_r as phototag_psfmag_r,
	pt.ra as phototag_ra,
	pt.type as phototag_type,
	pz.absmagr as photoz_absmagr,
	pz.photoerrorclass as photoz_photoerrorclass,
	pz.nncount as photoz_nncount,
	pz.nnvol as photoz_nnvol,
	pz.z as photoz_z,
	pz.zerr as photoz_zerr,
	sp.class as specphoto_class,
	sp.dec as specphoto_dec,
	sp.mode as specphoto_mode,
	sp.modelmag_r as specphoto_modelmag_r,
	sp.petromag_r as specphoto_petromag_r,
	sp.petromag_z as specphoto_petromag_z,
	sp.ra as specphoto_ra,
	sp.type as specphoto_type,
	sp.z as specphoto_z,
	sp.zwarning as specphoto_zwarning,
	spa.class as specphotoall_class,
	spa.dec as specphotoall_dec,
	spa.mode as specphotoall_mode,
	spa.modelmag_g as specphotoall_modelmag_g,
	spa.modelmag_i as specphotoall_modelmag_i,
	spa.modelmag_r as specphotoall_modelmag_r,
	spa.modelmag_u as specphotoall_modelmag_u,
	spa.modelmag_z as specphotoall_modelmag_z,
	spa.petromag_u as specphotoall_petromag_u,
	spa.petromag_r as specphotoall_petromag_r,
	spa.ra as specphotoall_ra,
	spa.sourcetype as specphotoall_sourcetype,
	spa.type as specphotoall_type,
	spa.z as specphotoall_z,
	spa.zwarning as specphotoall_zwarning,
	sps.fehadop as sppparams_fehadop,
	sps.spectypesubclass as sppparams_spectypesubclass,
	smfged.logmass as stellarmassfspsgranearlydust_logmass,
	smfged.z as stellarmassfspsgranearlydust_z,
	zs.elliptical as zoospec_elliptical,
	zs.p_cs as zoospec_p_cs,
	zs.p_cs_debiased as zoospec_p_cs_debiased,
	zs.p_el as zoospec_p_el,
	zs.p_el_debiased as zoospec_p_el_debiased,
	zs.spiral as zoospec_spiral,
	zs.uncertain as zoospec_uncertain

FROM photoobjall poa
JOIN photoobj po on po.objid = poa.objid
JOIN galaxy g on g.objid = poa.objid
JOIN galaxytag gt on gt.objid = poa.objid
JOIN galspecextra gse on gse.specobjid = poa.specobjid
JOIN galspecindx gsi on gsi.specobjid = poa.specobjid
JOIN galspecline gs on gs.specobjid = poa.specobjid
JOIN phototag pt on pt.objid = poa.objid
JOIN photoz pz on pz.objid = poa.objid
JOIN specphoto sp on sp.objid = poa.objid
JOIN specphotoall spa on spa.objid = poa.objid
JOIN sppparams sps on sps.bestobjid = poa.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid
JOIN zoospec zs on zs.objid = poa.objid

-- String version
SELECT g.clean as galaxy_clean, g.dec as galaxy_dec, g.g as galaxy_g, g.petromag_r as galaxy_petromag_r, g.petromag_u as galaxy_petromag_u, g.petror90_g as galaxy_petror90_g, g.petror90_r as galaxy_petror90_r, g.petrorad_u as galaxy_petrorad_u, g.r as galaxy_r, g.ra as galaxy_ra, gt.dec as galaxytag_dec, gt.ra as galaxytag_ra, gt.type as galaxytag_type, gse.bptclass as galspecextra_bptclass, gse.sfr_fib_p50 as galspecextra_sfr_fib_p50, gse.sfr_tot_p50 as galspecextra_sfr_tot_p50, gse.sfr_tot_p84 as galspecextra_sfr_tot_p84, gse.specsfr_tot_p50 as galspecextra_specsfr_tot_p50, gsi.d4000_n as galspecindx_d4000_n, gs.h_alpha_eqw as galspecline_h_alpha_eqw, gs.h_alpha_flux as galspecline_h_alpha_flux, gs.h_alpha_flux_err as galspecline_h_alpha_flux_err, gs.h_beta_eqw as galspecline_h_beta_eqw, gs.h_beta_flux as galspecline_h_beta_flux, gs.h_beta_flux_err as galspecline_h_beta_flux_err, gs.nii_6584_flux as galspecline_nii_6584_flux, gs.oi_6300_flux_err as galspecline_oi_6300_flux_err, gs.oiii_5007_eqw as galspecline_oiii_5007_eqw, gs.oiii_5007_flux as galspecline_oiii_5007_flux, gs.sii_6717_flux as galspecline_sii_6717_flux, gs.sii_6731_flux_err as galspecline_sii_6731_flux_err, po.b as photoobj_b, po.camcol as photoobj_camcol, po.clean as photoobj_clean, po.cmodelmag_g as photoobj_cmodelmag_g, po.dec as photoobj_dec, po.devrad_g as photoobj_devrad_g, po.devrad_r as photoobj_devrad_r, po.fibermag_r as photoobj_fibermag_r, po.field as photoobj_field, po.flags as photoobj_flags, po.fracdev_r as photoobj_fracdev_r, po.g as photoobj_g, po.l as photoobj_l, po.mode as photoobj_mode, po.petromag_r as photoobj_petromag_r, po.petromag_z as photoobj_petromag_z, po.petror50_g as photoobj_petror50_g, po.petror50_r as photoobj_petror50_r, po.petrorad_g as photoobj_petrorad_g, po.petrorad_r as photoobj_petrorad_r, po.r as photoobj_r, po.ra as photoobj_ra, po.run as photoobj_run, po.type as photoobj_type, po.u as photoobj_u, poa.camcol as photoobjall_camcol, poa.clean as photoobjall_clean, poa.dec as photoobjall_dec, poa.dered_r as photoobjall_dered_r, poa.devrad_r as photoobjall_devrad_r, poa.devraderr_r as photoobjall_devraderr_r, poa.exprad_r as photoobjall_exprad_r, poa.field as photoobjall_field, poa.fracdev_r as photoobjall_fracdev_r, poa.mode as photoobjall_mode, poa.petromag_r as photoobjall_petromag_r, poa.ra as photoobjall_ra, poa.run as photoobjall_run, poa.type as photoobjall_type, poa.u as photoobjall_u, pt.clean as phototag_clean, pt.dec as phototag_dec, pt.mode as phototag_mode, pt.nchild as phototag_nchild, pt.psfmag_r as phototag_psfmag_r, pt.ra as phototag_ra, pt.type as phototag_type, pz.absmagr as photoz_absmagr, pz.photoerrorclass as photoz_photoerrorclass, pz.nncount as photoz_nncount, pz.nnvol as photoz_nnvol, pz.z as photoz_z, pz.zerr as photoz_zerr, sp.class as specphoto_class, sp.dec as specphoto_dec, sp.mode as specphoto_mode, sp.modelmag_r as specphoto_modelmag_r, sp.petromag_r as specphoto_petromag_r, sp.petromag_z as specphoto_petromag_z, sp.ra as specphoto_ra, sp.type as specphoto_type, sp.z as specphoto_z, sp.zwarning as specphoto_zwarning, spa.class as specphotoall_class, spa.dec as specphotoall_dec, spa.mode as specphotoall_mode, spa.modelmag_g as specphotoall_modelmag_g, spa.modelmag_i as specphotoall_modelmag_i, spa.modelmag_r as specphotoall_modelmag_r, spa.modelmag_u as specphotoall_modelmag_u, spa.modelmag_z as specphotoall_modelmag_z, spa.petromag_u as specphotoall_petromag_u, spa.petromag_r as specphotoall_petromag_r, spa.ra as specphotoall_ra, spa.sourcetype as specphotoall_sourcetype, spa.type as specphotoall_type, spa.z as specphotoall_z, spa.zwarning as specphotoall_zwarning, sps.fehadop as sppparams_fehadop, sps.spectypesubclass as sppparams_spectypesubclass, smfged.logmass as stellarmassfspsgranearlydust_logmass, smfged.z as stellarmassfspsgranearlydust_z, zs.elliptical as zoospec_elliptical, zs.p_cs as zoospec_p_cs, zs.p_cs_debiased as zoospec_p_cs_debiased, zs.p_el as zoospec_p_el, zs.p_el_debiased as zoospec_p_el_debiased, zs.spiral as zoospec_spiral, zs.uncertain as zoospec_uncertain FROM photoobjall poa JOIN photoobj po on po.objid = poa.objid JOIN galaxy g on g.objid = poa.objid JOIN galaxytag gt on gt.objid = poa.objid JOIN galspecextra gse on gse.specobjid = poa.specobjid JOIN galspecindx gsi on gsi.specobjid = poa.specobjid JOIN galspecline gs on gs.specobjid = poa.specobjid JOIN phototag pt on pt.objid = poa.objid JOIN photoz pz on pz.objid = poa.objid JOIN specphoto sp on sp.objid = poa.objid JOIN specphotoall spa on spa.objid = poa.objid JOIN sppparams sps on sps.bestobjid = poa.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid JOIN zoospec zs on zs.objid = poa.objid