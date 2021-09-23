CREATE TABLE phototag (
objid bigint,
skyversion tinyint,
run smallint,
rerun smallint,
camcol tinyint,
field smallint,
obj smallint,
mode tinyint,
nchild smallint,
type smallint,
clean integer,
probpsf real,
insidemask tinyint,
flags bigint,
flags_u bigint,
flags_g bigint,
flags_r bigint,
flags_i bigint,
flags_z bigint,
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
petror50_r real,
petror90_r real,
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
mrrccpsf_r real,
fracdev_u real,
fracdev_g real,
fracdev_r real,
fracdev_i real,
fracdev_z real,
psffwhm_u real,
psffwhm_g real,
psffwhm_r real,
psffwhm_i real,
psffwhm_z real,
resolvestatus integer,
thingid integer,
balkanid integer,
nobserve integer,
ndetect integer,
calibstatus_u integer,
calibstatus_g integer,
calibstatus_r integer,
calibstatus_i integer,
calibstatus_z integer,
ra float,
`dec` float,
cx float,
cy float,
cz float,
extinction_u real,
extinction_g real,
extinction_r real,
extinction_i real,
extinction_z real,
htmid bigint,
fieldid bigint,
specobjid bigint,
size float,
Foreign Key (objid) references photoobjall (objid),
unique(objid)
);