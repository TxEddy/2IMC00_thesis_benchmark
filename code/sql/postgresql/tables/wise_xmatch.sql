CREATE TABLE wise_xmatch (
sdss_objid bigint references photoobj (objid),
wise_cntr bigint,
match_dist real
);