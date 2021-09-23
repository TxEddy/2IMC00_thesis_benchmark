CREATE TABLE stellarmassstarformingport (
specobjid numeric,
plate smallint,
fiberid smallint,
mjd integer,
ra numeric,
dec numeric,
z real,
zerr real,
logmass real,
minlogmass real,
maxlogmass real,
medianpdf real,
pdf16 real,
pdf84 real,
peakpdf real,
logmass_nomassloss real,
minlogmass_nomassloss real,
maxlogmass_nomassloss real,
medianpdf_nomassloss real,
pdf16_nomassloss real,
pdf84_nomassloss real,
peakpdf_nomassloss real,
reducedchi2 real,
age real,
minage real,
maxage real,
sfr real,
minsfr real,
maxsfr real,
absmagk real,
sfh varchar(32),
metallicity varchar(32),
reddeninglaw smallint,
nfilter smallint
);