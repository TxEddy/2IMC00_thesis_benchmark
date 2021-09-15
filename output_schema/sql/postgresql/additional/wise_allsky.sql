CREATE TABLE wise_allsky (
cntr bigint,
ra numeric,
dec numeric,
sigra real,
sigdec real,
sigradec real,
wx real,
wy real,
coadd_id varchar(13),
src integer,
rchi2 real,
xsc_prox real,
tmass_key integer,
r_2mass real,
pa_2mass real,
n_2mass smallint,
j_m_2mass real,
j_msig_2mass real,
h_m_2mass real,
h_msig_2mass real,
k_m_2mass real,
k_msig_2mass real,
rho12 smallint,
rho23 smallint,
rho34 smallint,
q12 smallint,
q23 smallint,
q34 smallint,
blend_ext_flags smallint,
w1mpro real,
w1sigmpro real,
w1snr real,
w1rchi2 real,
w1sat real,
w1nm smallint,
w1m smallint,
w1cov real,
w1frtr integer,
w1flux real,
w1sigflux real,
w1sky real,
w1sigsky real,
w1conf real,
w1mag real,
w1sigmag real,
w1mcor real,
w1magp real,
w1sigp1 real,
w1sigp2 real,
w1dmag real,
w1mjdmin numeric,
w1mjdmax numeric,
w1mjdmean numeric,
w1rsemi real,
w1ba real,
w1pa real,
w1gmag real,
w1siggmag real,
w1flg smallint,
w1gflg smallint,
ph_qual_det1 smallint,
w1ndf smallint,
w1mlq smallint,
w1cc_map smallint,
var_flg1 smallint,
moon_lev1 smallint,
satnum1 smallint,
w2mpro real,
w2sigmpro real,
w2snr real,
w2rchi2 real,
w2sat real,
w2nm smallint,
w2m smallint,
w2cov real,
w2frtr real,
w2flux real,
w2sigflux real,
w2sky real,
w2sigsky real,
w2conf real,
w2mag real,
w2sigmag real,
w2mcor real,
w2magp real,
w2sigp1 real,
w2sigp2 real,
w2dmag real,
w2mjdmin numeric,
w2mjdmax numeric,
w2mjdmean numeric,
w2rsemi real,
w2ba real,
w2pa real,
w2gmag real,
w2siggmag real,
w2flg smallint,
w2gflg smallint,
ph_qual_det2 smallint,
w2ndf smallint,
w2mlq smallint,
w2cc_map smallint,
var_flg2 smallint,
moon_lev2 smallint,
satnum2 smallint,
w3mpro real,
w3sigmpro real,
w3snr real,
w3rchi2 real,
w3sat real,
w3nm smallint,
w3m smallint,
w3cov real,
w3frtr real,
w3flux real,
w3sigflux real,
w3sky real,
w3sigsky real,
w3conf real,
w3mag real,
w3sigmag real,
w3mcor real,
w3magp real,
w3sigp1 real,
w3sigp2 real,
w3dmag real,
w3mjdmin numeric,
w3mjdmax numeric,
w3mjdmean numeric,
w3rsemi real,
w3ba real,
w3pa real,
w3gmag real,
w3siggmag real,
w3flg smallint,
w3gflg smallint,
ph_qual_det3 smallint,
w3ndf smallint,
w3mlq smallint,
w3cc_map smallint,
var_flg3 smallint,
moon_lev3 smallint,
satnum3 smallint,
w4mpro real,
w4sigmpro real,
w4snr real,
w4rchi2 real,
w4sat real,
w4nm smallint,
w4m smallint,
w4cov real,
w4frtr real,
w4flux real,
w4sigflux real,
w4sky real,
w4sigsky real,
w4conf real,
w4mag real,
w4sigmag real,
w4mcor real,
w4magp real,
w4sigp1 real,
w4sigp2 real,
w4dmag real,
w4mjdmin numeric,
w4mjdmax numeric,
w4mjdmean numeric,
w4rsemi real,
w4ba real,
w4pa real,
w4gmag real,
w4siggmag real,
w4flg smallint,
w4gflg smallint,
ph_qual_det4 smallint,
w4ndf smallint,
w4mlq smallint,
w4cc_map smallint,
var_flg4 smallint,
moon_lev4 smallint,
satnum4 smallint,
w1mag_1 real,
w1sigmag_1 real,
w1flg_1 smallint,
w1mag_2 real,
w1sigmag_2 real,
w1flg_2 smallint,
w1mag_3 real,
w1sigmag_3 real,
w1flg_3 smallint,
w1mag_4 real,
w1sigmag_4 real,
w1flg_4 smallint,
w1mag_5 real,
w1sigmag_5 real,
w1flg_5 smallint,
w1mag_6 real,
w1sigmag_6 real,
w1flg_6 smallint,
w1mag_7 real,
w1sigmag_7 real,
w1flg_7 smallint,
w1mag_8 real,
w1sigmag_8 real,
w1flg_8 smallint,
w2mag_1 real,
w2sigmag_1 real,
w2flg_1 smallint,
w2mag_2 real,
w2sigmag_2 real,
w2flg_2 smallint,
w2mag_3 real,
w2sigmag_3 real,
w2flg_3 smallint,
w2mag_4 real,
w2sigmag_4 real,
w2flg_4 smallint,
w2mag_5 real,
w2sigmag_5 real,
w2flg_5 smallint,
w2mag_6 real,
w2sigmag_6 real,
w2flg_6 smallint,
w2mag_7 real,
w2sigmag_7 real,
w2flg_7 smallint,
w2mag_8 real,
w2sigmag_8 real,
w2flg_8 smallint,
w3mag_1 real,
w3sigmag_1 real,
w3flg_1 smallint,
w3mag_2 real,
w3sigmag_2 real,
w3flg_2 smallint,
w3mag_3 real,
w3sigmag_3 real,
w3flg_3 smallint,
w3mag_4 real,
w3sigmag_4 real,
w3flg_4 smallint,
w3mag_5 real,
w3sigmag_5 real,
w3flg_5 smallint,
w3mag_6 real,
w3sigmag_6 real,
w3flg_6 smallint,
w3mag_7 real,
w3sigmag_7 real,
w3flg_7 smallint,
w3mag_8 real,
w3sigmag_8 real,
w3flg_8 smallint,
w4mag_1 real,
w4sigmag_1 real,
w4flg_1 smallint,
w4mag_2 real,
w4sigmag_2 real,
w4flg_2 smallint,
w4mag_3 real,
w4sigmag_3 real,
w4flg_3 smallint,
w4mag_4 real,
w4sigmag_4 real,
w4flg_4 smallint,
w4mag_5 real,
w4sigmag_5 real,
w4flg_5 smallint,
w4mag_6 real,
w4sigmag_6 real,
w4flg_6 smallint,
w4mag_7 real,
w4sigmag_7 real,
w4flg_7 smallint,
w4mag_8 real,
w4sigmag_8 real,
w4flg_8 smallint,
glat numeric,
glon numeric,
rjce real
);