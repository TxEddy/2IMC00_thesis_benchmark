-- Galaxy
select g.clean as galaxy_clean, g.dec as galaxy_dec, g.g as galaxy_g, g.petromag_r as galaxy_petromag_r, g.petromag_u as galaxy_petromag_u, g.petror90_g as galaxy_petror90_g, g.petror90_r as galaxy_petror90_r, g.petrorad_u as galaxy_petrorad_u, g.r as galaxy_r, g.ra as galaxy_ra from galaxy as g

-- Galaxytag
select gt.dec as galaxytag_dec, gt.ra as galaxytag_ra, gt.type as galaxytag_type from galaxytag gt

-- galSpecExtra
select gse.bptclass as galspecextra_bptclass, gse.sfr_fib_p50 as galspecextra_sfr_fib_p50, gse.sfr_tot_p50 as galspecextra_sfr_tot_p50, gse.sfr_tot_p84 as galspecextra_sfr_tot_p84, gse.specsfr_tot_p50 as galspecextra_specsfr_tot_p50 from galspecextra gse

-- galspecindx
select gsi.d4000_n as galspecindx_d4000_n from galspecindx gsi

-- galspecline
select gs.h_alpha_eqw as galspecline_h_alpha_eqw, gs.h_alpha_flux as galspecline_h_alpha_flux, gs.h_alpha_flux_err as galspecline_h_alpha_flux_err, gs.h_beta_eqw as galspecline_h_beta_eqw, gs.h_beta_flux as galspecline_h_beta_flux, gs.h_beta_flux_err as galspecline_h_beta_flux_err, gs.nii_6584_flux as galspecline_nii_6584_flux, gs.oi_6300_flux_err as galspecline_oi_6300_flux_err, gs.oiii_5007_eqw as galspecline_oiii_5007_eqw, gs.oiii_5007_flux as galspecline_oiii_5007_flux, gs.sii_6717_flux as galspecline_sii_6717_flux, gs.sii_6731_flux_err as galspecline_sii_6731_flux_err from galspecline gs

-- mangadrpall
select mda.nsa_sersic_mass as mangadrpall_nsa_sersic_mass, mda.nsa_sersic_n as mangadrpall_nsa_sersic_n, mda.nsa_sersic_th50 as mangadrpall_nsa_sersic_th50, mda.z as mangadrpall_z from mangadrpall mda

-- mangapipe3d
select mp3d.ellip, mp3d.e_log_mass, mp3d.e_log_mass_gas, mp3d.ifudsgn, mp3d.log_mass, mp3d.log_mass_gas, mp3d.log_sfr_ha, mp3d.plate, mp3d.plateifu, mp3d.re_arc, mp3d.re_kpc, mp3d.redshift from mangapipe3d mp3d

-- photoobj
select po.b as photoobj_b, po.camcol as photoobj_camcol, po.clean as photoobj_clean, po.cmodelmag_g as photoobj_cmodelmag_g, po.dec as photoobj_dec, po.devrad_g as photoobj_devrad_g, po.devrad_r as photoobj_devrad_r, po.fibermag_r as photoobj_fibermag_r, po.field as photoobj_field, po.flags as photoobj_flags, po.fracdev_r as photoobj_fracdev_r, po.g as photoobj_g, po.l as photoobj_l, po.mode as photoobj_mode, po.petromag_r as photoobj_petromag_r, po.petromag_z as photoobj_petromag_z, po.petror50_g as photoobj_petror50_g, po.petror50_r as photoobj_petror50_r, po.petrorad_g as photoobj_petrorad_g, po.petrorad_r as photoobj_petrorad_r, po.r as photoobj_r, po.ra as photoobj_ra, po.run as photoobj_run, po.type as photoobj_type, po.u as photoobj_u from photoobj po

-- photoobjall
select poa.camcol as photoobjall_camcol, poa.clean as photoobjall_clean, poa.dec as photoobjall_dec, poa.derad_r as photoobjall_derad_r, poa.devred_r as photoobjall_devred_r, poa.devraderr_r as photoobjall_devraderr_r, poa.exprad_r as photoobjall_exprad_r, poa.field as photoobjall_field, poa.fracdev_r as photoobjall_fracdev_r, poa.mode as photoobjall_mode, poa.petromag_r as photoobjall_petromag_r, poa.ra as photoobjall_ra, poa.run as photoobjall_run, poa.type as photoobjall_type, poa.u as photoobjall_u from photoobjall

-- phototag
select pt.clean as phototag_clean, pt.dec as phototag_dec, pt.mode as phototag_mode, pt.nchild as phototag_nchild, pt.psfmag_r as phototag_psfmag_r, pt.ra as phototag_ra, pt.type as phototag_type from phototag pt

-- photoz
select pz.absmagr as photoz_absmagr, pz.photoerrorclass as photoz_photoerrorclass, pz.nncount as photoz_nncount, pz.nnvol as photoz_nnvol, pz.z as photoz_z, pz.zerr as photoz_zerr, from photoz pz

-- specphoto
select sp.class as specphoto_class, sp.dec as specphoto_dec, sp.mode as specphoto_mode, sp.modelmag_r as specphoto_modelmag_r, sp.petromag_r as specphoto_petromag_r, sp.petromag_z as specphoto_petromag_z, sp.ra as specphoto_ra, sp.type as specphoto_type, sp.z as specphoto_z, sp.zwarning as specphoto_zwarning from specphoto sp

-- specphotoall
select spa.class as specphotoall_class, spa.dec as specphotoall_dec, spa.mode as specphotoall_mode, spa.modelmag_g as specphotoall_modelmag_g, spa.modelmag_i as specphotoall_modelmag_i, spa.modelmag_r as specphotoall_modelmag_r, spa.modelmag_u as specphotoall_modelmag_u, spa.modelmag_z as specphotoall_modelmag_z, spa.petromag_u as specphotoall_petromag_u, spa.petromag_r as specphotoall_petromag_r, spa.ra as specphotoall_ra, spa.sourcetype as specphotoall_sourcetype, spa.type as specphotoall_type, spa.z as specphotoall_z, spa.zwarning as specphotoall_zwarning from specphotoall spa

-- sppparams
select sps.fehadop as sppparams_fehadop, sps.spectypesubclass as sppparams_spectypesubclass, from sppparams sps

-- stellarmassfspsgranearlydust
select smfged.logmass as stellarmassfspsgranearlydust_logmass, smfged.z as stellarmassfspsgranearlydust_z from stellarmassfspsgranearlydust smfged

-- zoospec
select zs.elliptical as zoospec_elliptical, zs.p_cs as zoospec_p_cs, zs.p_cs_debiased as zoospec_p_cs_debiased, zs.p_el as zoospec_p_el, zs.p_el_debiased as zoospec_p_el_debiased, zs.spiral as zoospec_spiral, zs.uncertain as zoospec_uncertain from zoospec zs


--- complete query for correlation matrix
SELECT g.clean as galaxy_clean,
	g.dec as galaxy_dec,
	g.g as galaxy_g,
	g.petromag_r as galaxy_petromag_r,
	g.petromag_u as galaxy_petromag_u,
	g.petror90_g as galaxy_petror90_g,
	g.petror90_r as galaxy_petror90_r,
	g.petrorad_u as galaxy_petrorad_u,
	g.r as galaxy_r,
	g.ra as galaxy_ra,
	gt.dec as galaxytag_dec,
	gt.ra as galaxytag_ra,
	gt.type as galaxytag_type,
	gse.bptclass as galspecextra_bptclass,
	gse.sfr_fib_p50 as galspecextra_sfr_fib_p50,
	gse.sfr_tot_p50 as galspecextra_sfr_tot_p50,
	gse.sfr_tot_p84 as galspecextra_sfr_tot_p84,
	gse.specsfr_tot_p50 as galspecextra_specsfr_tot_p50,
	gsi.d4000_n as galspecindx_d4000_n,
	gs.h_alpha_eqw as galspecline_h_alpha_eqw,
	gs.h_alpha_flux as galspecline_h_alpha_flux,
	gs.h_alpha_flux_err as galspecline_h_alpha_flux_err,
	gs.h_beta_eqw as galspecline_h_beta_eqw,
	gs.h_beta_flux as galspecline_h_beta_flux,
	gs.h_beta_flux_err as galspecline_h_beta_flux_err,
	gs.nii_6584_flux as galspecline_nii_6584_flux,
	gs.oi_6300_flux_err as galspecline_oi_6300_flux_err,
	gs.oiii_5007_eqw as galspecline_oiii_5007_eqw,
	gs.oiii_5007_flux as galspecline_oiii_5007_flux,
	gs.sii_6717_flux as galspecline_sii_6717_flux,
	gs.sii_6731_flux_err as galspecline_sii_6731_flux_err,
	po.b as photoobj_b,
	po.camcol as photoobj_camcol,
	po.clean as photoobj_clean,
	po.cmodelmag_g as photoobj_cmodelmag_g,
	po.dec as photoobj_dec,
	po.devrad_g as photoobj_devrad_g,
	po.devrad_r as photoobj_devrad_r,
	po.fibermag_r as photoobj_fibermag_r,
	po.field as photoobj_field,
	po.flags as photoobj_flags,
	po.fracdev_r as photoobj_fracdev_r,
	po.g as photoobj_g,
	po.l as photoobj_l,
	po.mode as photoobj_mode,
	po.petromag_r as photoobj_petromag_r,
	po.petromag_z as photoobj_petromag_z,
	po.petror50_g as photoobj_petror50_g,
	po.petror50_r as photoobj_petror50_r,
	po.petrorad_g as photoobj_petrorad_g,
	po.petrorad_r as photoobj_petrorad_r,
	po.r as photoobj_r,
	po.ra as photoobj_ra,
	po.run as photoobj_run,
	po.type as photoobj_type,
	po.u as photoobj_u,
	poa.camcol as photoobjall_camcol,
	poa.clean as photoobjall_clean,
	poa.dec as photoobjall_dec,
	poa.dered_r as photoobjall_dered_r,
	poa.devrad_r as photoobjall_devrad_r,
	poa.devraderr_r as photoobjall_devraderr_r,
	poa.exprad_r as photoobjall_exprad_r,
	poa.field as photoobjall_field,
	poa.fracdev_r as photoobjall_fracdev_r,
	poa.mode as photoobjall_mode,
	poa.petromag_r as photoobjall_petromag_r,
	poa.ra as photoobjall_ra,
	poa.run as photoobjall_run,
	poa.type as photoobjall_type,
	poa.u as photoobjall_u,
	pt.clean as phototag_clean,
	pt.dec as phototag_dec,
	pt.mode as phototag_mode,
	pt.nchild as phototag_nchild,
	pt.psfmag_r as phototag_psfmag_r,
	pt.ra as phototag_ra,
	pt.type as phototag_type,
	pz.absmagr as photoz_absmagr,
	pz.photoerrorclass as photoz_photoerrorclass,
	pz.nncount as photoz_nncount,
	pz.nnvol as photoz_nnvol,
	pz.z as photoz_z,
	pz.zerr as photoz_zerr,
	sp.class as specphoto_class,
	sp.dec as specphoto_dec,
	sp.mode as specphoto_mode,
	sp.modelmag_r as specphoto_modelmag_r,
	sp.petromag_r as specphoto_petromag_r,
	sp.petromag_z as specphoto_petromag_z,
	sp.ra as specphoto_ra,
	sp.type as specphoto_type,
	sp.z as specphoto_z,
	sp.zwarning as specphoto_zwarning,
	spa.class as specphotoall_class,
	spa.dec as specphotoall_dec,
	spa.mode as specphotoall_mode,
	spa.modelmag_g as specphotoall_modelmag_g,
	spa.modelmag_i as specphotoall_modelmag_i,
	spa.modelmag_r as specphotoall_modelmag_r,
	spa.modelmag_u as specphotoall_modelmag_u,
	spa.modelmag_z as specphotoall_modelmag_z,
	spa.petromag_u as specphotoall_petromag_u,
	spa.petromag_r as specphotoall_petromag_r,
	spa.ra as specphotoall_ra,
	spa.sourcetype as specphotoall_sourcetype,
	spa.type as specphotoall_type,
	spa.z as specphotoall_z,
	spa.zwarning as specphotoall_zwarning,
	sps.fehadop as sppparams_fehadop,
	sps.spectypesubclass as sppparams_spectypesubclass,
	smfged.logmass as stellarmassfspsgranearlydust_logmass,
	smfged.z as stellarmassfspsgranearlydust_z,
	zs.elliptical as zoospec_elliptical,
	zs.p_cs as zoospec_p_cs,
	zs.p_cs_debiased as zoospec_p_cs_debiased,
	zs.p_el as zoospec_p_el,
	zs.p_el_debiased as zoospec_p_el_debiased,
	zs.spiral as zoospec_spiral,
	zs.uncertain as zoospec_uncertain

FROM photoobjall poa
JOIN photoobj po on po.objid = poa.objid
JOIN galaxy g on g.objid = poa.objid
JOIN galaxytag gt on gt.objid = poa.objid
JOIN galspecextra gse on gse.specobjid = poa.specobjid
JOIN galspecindx gsi on gsi.specobjid = poa.specobjid
JOIN galspecline gs on gs.specobjid = poa.specobjid
JOIN phototag pt on pt.objid = poa.objid
JOIN photoz pz on pz.objid = poa.objid
JOIN specphoto sp on sp.objid = poa.objid
JOIN specphotoall spa on spa.objid = poa.objid
JOIN sppparams sps on sps.bestobjid = poa.objid
JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid
JOIN zoospec zs on zs.objid = poa.objid





