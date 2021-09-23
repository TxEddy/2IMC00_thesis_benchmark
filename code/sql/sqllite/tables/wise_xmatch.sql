CREATE TABLE wise_xmatch (
sdss_objid integer,
wise_cntr integer,
match_dist real,
Foreign Key (sdss_objid) references photoobjall (objid)
);