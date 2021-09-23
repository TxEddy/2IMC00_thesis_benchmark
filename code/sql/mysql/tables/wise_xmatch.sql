CREATE TABLE wise_xmatch (
sdss_objid bigint,
wise_cntr bigint,
match_dist real,
Foreign Key (sdss_objid) references photoobj (objid)
);