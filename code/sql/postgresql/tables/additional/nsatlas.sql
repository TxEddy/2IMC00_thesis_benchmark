CREATE TABLE nsatlas (
nsaid integer,
iauname varchar(32),
subdir varchar(128),
ra numeric,
dec numeric,
isdss integer,
ined integer,
isixdf integer,
ialfalfa integer,
izcat integer,
itwodf integer,
mag real,
z real,
zsrc varchar(32),
size real,
run smallint,
camcol smallint,
field smallint,
rerun varchar(32),
xpos real,
ypos real,
zdist real,
plate integer,
fiberid smallint,
mjd integer,
racat numeric,
deccat numeric,
survey varchar(32),
programname varchar(32),
platequality varchar(32),
tile integer,
plug_ra numeric,
plug_dec numeric,
in_dr7_lss integer,
elpetro_ba real,
elpetro_phi real,
elpetro_theta real,
elpetro_theta_r_original real,
elpetro_flux_f real,
elpetro_flux_n real,
elpetro_flux_u real,
elpetro_flux_g real,
elpetro_flux_r real,
elpetro_flux_i real,
elpetro_flux_z real,
elpetro_flux_r_original real,
elpetro_flux_ivar_f real,
elpetro_flux_ivar_n real,
elpetro_flux_ivar_u real,
elpetro_flux_ivar_g real,
elpetro_flux_ivar_r real,
elpetro_flux_ivar_i real,
elpetro_flux_ivar_z real,
elpetro_flux_ivar_r_original real,
elpetro_th50_f real,
elpetro_th50_n real,
elpetro_th50_u real,
elpetro_th50_g real,
elpetro_th50_r real,
elpetro_th50_i real,
elpetro_th50_z real,
elpetro_th90_f real,
elpetro_th50_r_original real,
elpetro_th90_n real,
elpetro_th90_u real,
elpetro_th90_g real,
elpetro_th90_r real,
elpetro_th90_i real,
elpetro_th90_z real,
elpetro_th90_r_original real,
elpetro_nmgy_f real,
elpetro_nmgy_n real,
elpetro_nmgy_u real,
elpetro_nmgy_g real,
elpetro_nmgy_r real,
elpetro_nmgy_i real,
elpetro_nmgy_z real,
elpetro_nmgy_ivar_f real,
elpetro_nmgy_ivar_n real,
elpetro_nmgy_ivar_u real,
elpetro_nmgy_ivar_g real,
elpetro_nmgy_ivar_r real,
elpetro_nmgy_ivar_i real,
elpetro_nmgy_ivar_z real,
elpetro_ok smallint,
elpetro_rnmgy_f real,
elpetro_rnmgy_n real,
elpetro_rnmgy_u real,
elpetro_rnmgy_g real,
elpetro_rnmgy_r real,
elpetro_rnmgy_i real,
elpetro_rnmgy_z real,
elpetro_absmag_f real,
elpetro_absmag_n real,
elpetro_absmag_u real,
elpetro_absmag_g real,
elpetro_absmag_r real,
elpetro_absmag_i real,
elpetro_absmag_z real,
elpetro_amivar_f real,
elpetro_amivar_n real,
elpetro_amivar_u real,
elpetro_amivar_g real,
elpetro_amivar_r real,
elpetro_amivar_i real,
elpetro_amivar_z real,
elpetro_kcorrect_f real,
elpetro_kcorrect_n real,
elpetro_kcorrect_u real,
elpetro_kcorrect_g real,
elpetro_kcorrect_r real,
elpetro_kcorrect_i real,
elpetro_kcorrect_z real,
elpetro_kcoeff_0 real,
elpetro_kcoeff_1 real,
elpetro_kcoeff_2 real,
elpetro_kcoeff_3 real,
elpetro_kcoeff_4 real,
elpetro_mass real,
elpetro_mtol_f real,
elpetro_mtol_n real,
elpetro_mtol_u real,
elpetro_mtol_g real,
elpetro_mtol_r real,
elpetro_mtol_i real,
elpetro_mtol_z real,
elpetro_b300 real,
elpetro_b1000 real,
elpetro_mets real,
petro_theta real,
petro_th50 real,
petro_th90 real,
petro_ba50 real,
petro_phi50 real,
petro_ba90 real,
petro_phi90 real,
petro_flux_f real,
petro_flux_n real,
petro_flux_u real,
petro_flux_g real,
petro_flux_r real,
petro_flux_i real,
petro_flux_z real,
petro_flux_ivar_f real,
petro_flux_ivar_n real,
petro_flux_ivar_u real,
petro_flux_ivar_g real,
petro_flux_ivar_r real,
petro_flux_ivar_i real,
petro_flux_ivar_z real,
fiber_flux_f real,
fiber_flux_n real,
fiber_flux_u real,
fiber_flux_g real,
fiber_flux_r real,
fiber_flux_i real,
fiber_flux_z real,
fiber_flux_ivar_f real,
fiber_flux_ivar_n real,
fiber_flux_ivar_u real,
fiber_flux_ivar_g real,
fiber_flux_ivar_r real,
fiber_flux_ivar_i real,
fiber_flux_ivar_z real,
sersic_n real,
sersic_ba real,
sersic_phi real,
sersic_th50 real,
sersic_flux_f real,
sersic_flux_n real,
sersic_flux_u real,
sersic_flux_g real,
sersic_flux_r real,
sersic_flux_i real,
sersic_flux_z real,
sersic_flux_ivar_f real,
sersic_flux_ivar_n real,
sersic_flux_ivar_u real,
sersic_flux_ivar_g real,
sersic_flux_ivar_r real,
sersic_flux_ivar_i real,
sersic_flux_ivar_z real,
sersic_nmgy_f real,
sersic_nmgy_n real,
sersic_nmgy_u real,
sersic_nmgy_g real,
sersic_nmgy_r real,
sersic_nmgy_i real,
sersic_nmgy_z real,
sersic_nmgy_ivar_f real,
sersic_nmgy_ivar_n real,
sersic_nmgy_ivar_u real,
sersic_nmgy_ivar_g real,
sersic_nmgy_ivar_r real,
sersic_nmgy_ivar_i real,
sersic_nmgy_ivar_z real,
sersic_ok integer,
sersic_rnmgy_f real,
sersic_rnmgy_n real,
sersic_rnmgy_u real,
sersic_rnmgy_g real,
sersic_rnmgy_r real,
sersic_rnmgy_i real,
sersic_rnmgy_z real,
sersic_absmag_f real,
sersic_absmag_n real,
sersic_absmag_u real,
sersic_absmag_g real,
sersic_absmag_r real,
sersic_absmag_i real,
sersic_absmag_z real,
sersic_amivar_f real,
sersic_amivar_n real,
sersic_amivar_u real,
sersic_amivar_g real,
sersic_amivar_r real,
sersic_amivar_i real,
sersic_amivar_z real,
sersic_kcorrect_f real,
sersic_kcorrect_n real,
sersic_kcorrect_u real,
sersic_kcorrect_g real,
sersic_kcorrect_r real,
sersic_kcorrect_i real,
sersic_kcorrect_z real,
sersic_kcoeff_0 real,
sersic_kcoeff_1 real,
sersic_kcoeff_2 real,
sersic_kcoeff_3 real,
sersic_kcoeff_4 real,
sersic_mass real,
sersic_mtol_f real,
sersic_mtol_n real,
sersic_mtol_u real,
sersic_mtol_g real,
sersic_mtol_r real,
sersic_mtol_i real,
sersic_mtol_z real,
sersic_b300 real,
sersic_b1000 real,
sersic_mets real,
asymmetry_f real,
asymmetry_n real,
asymmetry_u real,
asymmetry_g real,
asymmetry_r real,
asymmetry_i real,
asymmetry_z real,
clumpy_f real,
clumpy_n real,
clumpy_u real,
clumpy_g real,
clumpy_r real,
clumpy_i real,
clumpy_z real,
extinction_f real,
extinction_n real,
extinction_u real,
extinction_g real,
extinction_r real,
extinction_i real,
extinction_z real,
aid integer,
pid integer,
xcen numeric,
ycen numeric,
dflags_f integer,
dflags_n integer,
dflags_u integer,
dflags_g integer,
dflags_r integer,
dflags_i integer,
dflags_z integer,
dversion varchar(32)
);