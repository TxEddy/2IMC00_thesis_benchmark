CREATE TABLE specphotoall (
specobjid numeric,
mjd integer,
plate integer,
tile integer,
fiberid integer,
z real,
zerr real,
class text,
subclass text,
zwarning integer,
ra real,
dec real,
cx real,
cy real,
cz real,
htmid integer,
scienceprimary integer,
legacyprimary integer,
segueprimary integer,
segue1primary integer,
segue2primary integer,
bossprimary integer,
sdssprimary integer,
survey text,
programname text,
legacy_target1 integer,
legacy_target2 integer,
special_target1 integer,
special_target2 integer,
segue1_target1 integer,
segue1_target2 integer,
segue2_target1 integer,
segue2_target2 integer,
boss_target1 integer,
ancillary_target1 integer,
ancillary_target2 integer,
plateid numeric,
sourcetype text,
targetobjid integer,
objid integer,
skyversion integer,
run integer,
rerun integer,
camcol integer,
field integer,
obj integer,
mode integer,
nchild integer,
type integer,
flags integer,
psfmag_u real,
psfmag_g real,
psfmag_r real,
psfmag_i real,
psfmag_z real,
psfmagerr_u real,
psfmagerr_g real,
psfmagerr_r real,
psfmagerr_i real,
psfmagerr_z real,
fibermag_u real,
fibermag_g real,
fibermag_r real,
fibermag_i real,
fibermag_z real,
fibermagerr_u real,
fibermagerr_g real,
fibermagerr_r real,
fibermagerr_i real,
fibermagerr_z real,
petromag_u real,
petromag_g real,
petromag_r real,
petromag_i real,
petromag_z real,
petromagerr_u real,
petromagerr_g real,
petromagerr_r real,
petromagerr_i real,
petromagerr_z real,
modelmag_u real,
modelmag_g real,
modelmag_r real,
modelmag_i real,
modelmag_z real,
modelmagerr_u real,
modelmagerr_g real,
modelmagerr_r real,
modelmagerr_i real,
modelmagerr_z real,
cmodelmag_u real,
cmodelmag_g real,
cmodelmag_r real,
cmodelmag_i real,
cmodelmag_z real,
cmodelmagerr_u real,
cmodelmagerr_g real,
cmodelmagerr_r real,
cmodelmagerr_i real,
cmodelmagerr_z real,
mrrcc_r real,
mrrccerr_r real,
score real,
resolvestatus integer,
calibstatus_u integer,
calibstatus_g integer,
calibstatus_r integer,
calibstatus_i integer,
calibstatus_z integer,
photora real,
photodec real,
extinction_u real,
extinction_g real,
extinction_r real,
extinction_i real,
extinction_z real,
fieldid integer,
dered_u real,
dered_g real,
dered_r real,
dered_i real,
dered_z real,
Primary Key (specobjid)
);