CREATE TABLE platex (
plateid numeric,
firstrelease varchar(32),
plate smallint,
mjd integer,
mjdlist varchar(512),
survey varchar(32),
programname varchar(32),
instrument varchar(32),
chunk varchar(32),
platerun varchar(128),
designcomments varchar(128),
platequality varchar(32),
qualitycomments varchar(100),
platesn2 real,
deredsn2 real,
ra float,
dec float,
run2d varchar(32),
run1d varchar(32),
runsspp varchar(32),
tile smallint,
designid integer,
locationid integer,
iopversion varchar(64),
camversion varchar(64),
taihms varchar(64),
dateobs varchar(32),
timesys varchar(8),
cx float,
cy float,
cz float,
cartridgeid smallint,
tai float,
taibegin float,
taiend float,
airmass real,
mapmjd integer,
mapname varchar(32),
plugfile varchar(32),
exptime real,
exptimeb1 real,
exptimeb2 real,
exptimer1 real,
exptimer2 real,
vers2d varchar(32),
verscomb varchar(32),
vers1d varchar(32),
snturnoff real,
nturnoff real,
nexp smallint,
nexpb1 smallint,
nexpb2 smallint,
nexpr1 smallint,
nexpr2 smallint,
sn1_g real,
sn1_r real,
sn1_i real,
sn2_g real,
sn2_r real,
sn2_i real,
dered_sn1_g real,
dered_sn1_r real,
dered_sn1_i real,
dered_sn2_g real,
dered_sn2_r real,
dered_sn2_i real,
heliorv real,
goffstd real,
grmsstd real,
roffstd real,
rrmsstd real,
ioffstd real,
irmsstd real,
groffstd real,
grrmsstd real,
rioffstd real,
rirmsstd real,
goffgal real,
grmsgal real,
roffgal real,
rrmsgal real,
ioffgal real,
irmsgal real,
groffgal real,
grrmsgal real,
rioffgal real,
rirmsgal real,
nguide real,
seeing20 real,
seeing50 real,
seeing80 real,
rmsoff20 real,
rmsoff50 real,
rmsoff80 real,
airtemp real,
sfd_used tinyint,
xsigma real,
xsigmin real,
xsigmax real,
wsigma real,
wsigmin real,
wsigmax real,
xchi2 real,
xchi2min real,
xchi2max real,
skychi2 real,
skychi2min real,
skychi2max real,
fbadpix real,
fbadpix1 real,
fbadpix2 real,
status2d varchar(32),
statuscombine varchar(32),
status1d varchar(32),
ntotal integer,
ngalaxy integer,
nqso integer,
nstar integer,
nsky integer,
nunknown integer,
isbest tinyint,
isprimary tinyint,
istile tinyint,
ha real,
mjddesign integer,
theta real,
fscanversion varchar(32),
fmapversion varchar(32),
fscanmode varchar(32),
fscanspeed integer,
htmid bigint,
loadversion integer
);