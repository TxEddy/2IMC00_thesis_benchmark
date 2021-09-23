CREATE TABLE zoospec (
specobjid bigint,
objid bigint,
dr7objid bigint,
ra real,
dec real,
rastring varchar(11),
decstring varchar(11),
nvote integer,
p_el float,
p_cw float,
p_acw float,
p_edge float,
p_dk float,
p_mg float,
p_cs float,
p_el_debiased float,
p_cs_debiased float,
spiral integer,
elliptical integer,
uncertain integer,
Foreign Key (objid) references photoobjall (objid)
);