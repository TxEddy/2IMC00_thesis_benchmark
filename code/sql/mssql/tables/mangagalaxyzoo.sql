CREATE TABLE mangagalaxyzoo (
nsa_id integer,
iauname varchar(32),
ifudesignsize integer,
ifu_dec float,
ifu_ra float,
mangaid varchar(16),
manga_tileid integer,
object_dec float,
object_ra float,
survey varchar(128),
t01_smooth_or_features_a01_smooth_count real,
t01_smooth_or_features_a01_smooth_count_fraction real,
t01_smooth_or_features_a01_smooth_debiased real,
t01_smooth_or_features_a01_smooth_weight real,
t01_smooth_or_features_a01_smooth_weight_fraction real,
t01_smooth_or_features_a02_features_or_disk_count real,
t01_smooth_or_features_a02_features_or_disk_count_fraction real,
t01_smooth_or_features_a02_features_or_disk_debiased real,
t01_smooth_or_features_a02_features_or_disk_weight real,
t01_smooth_or_features_a02_features_or_disk_weight_fraction real,
t01_smooth_or_features_a03_star_or_artifact_count real,
t01_smooth_or_features_a03_star_or_artifact_count_fraction real,
t01_smooth_or_features_a03_star_or_artifact_debiased real,
t01_smooth_or_features_a03_star_or_artifact_weight real,
t01_smooth_or_features_a03_star_or_artifact_weight_fraction real,
t01_smooth_or_features_count real,
t01_smooth_or_features_weight real,
t02_edgeon_a04_yes_count real,
t02_edgeon_a04_yes_count_fraction real,
t02_edgeon_a04_yes_debiased real,
t02_edgeon_a04_yes_weight real,
t02_edgeon_a04_yes_weight_fraction real,
t02_edgeon_a05_no_count real,
t02_edgeon_a05_no_count_fraction real,
t02_edgeon_a05_no_debiased real,
t02_edgeon_a05_no_weight real,
t02_edgeon_a05_no_weight_fraction real,
t02_edgeon_count real,
t02_edgeon_weight real,
t03_bar_a06_bar_count real,
t03_bar_a06_bar_count_fraction real,
t03_bar_a06_bar_debiased real,
t03_bar_a06_bar_weight real,
t03_bar_a06_bar_weight_fraction real,
t03_bar_a07_no_bar_count real,
t03_bar_a07_no_bar_count_fraction real,
t03_bar_a07_no_bar_debiased real,
t03_bar_a07_no_bar_weight real,
t03_bar_a07_no_bar_weight_fraction real,
t03_bar_count real,
t03_bar_weight real,
t04_spiral_a08_spiral_count real,
t04_spiral_a08_spiral_count_fraction real,
t04_spiral_a08_spiral_debiased real,
t04_spiral_a08_spiral_weight real,
t04_spiral_a08_spiral_weight_fraction real,
t04_spiral_a09_no_spiral_count real,
t04_spiral_a09_no_spiral_count_fraction real,
t04_spiral_a09_no_spiral_debiased real,
t04_spiral_a09_no_spiral_weight real,
t04_spiral_a09_no_spiral_weight_fraction real,
t04_spiral_count real,
t04_spiral_weight real,
t05_bulge_prominence_a10_no_bulge_count real,
t05_bulge_prominence_a10_no_bulge_count_fraction real,
t05_bulge_prominence_a10_no_bulge_debiased real,
t05_bulge_prominence_a10_no_bulge_weight real,
t05_bulge_prominence_a10_no_bulge_weight_fraction real,
t05_bulge_prominence_a11_just_noticeable_count real,
t05_bulge_prominence_a11_just_noticeable_count_fraction real,
t05_bulge_prominence_a11_just_noticeable_debiased real,
t05_bulge_prominence_a11_just_noticeable_weight real,
t05_bulge_prominence_a11_just_noticeable_weight_fraction real,
t05_bulge_prominence_a12_obvious_count real,
t05_bulge_prominence_a12_obvious_count_fraction real,
t05_bulge_prominence_a12_obvious_debiased real,
t05_bulge_prominence_a12_obvious_weight real,
t05_bulge_prominence_a12_obvious_weight_fraction real,
t05_bulge_prominence_a13_dominant_count real,
t05_bulge_prominence_a13_dominant_count_fraction real,
t05_bulge_prominence_a13_dominant_debiased real,
t05_bulge_prominence_a13_dominant_weight real,
t05_bulge_prominence_a13_dominant_weight_fraction real,
t05_bulge_prominence_count real,
t05_bulge_prominence_weight real,
t06_odd_a14_yes_count real,
t06_odd_a14_yes_count_fraction real,
t06_odd_a14_yes_debiased real,
t06_odd_a14_yes_weight real,
t06_odd_a14_yes_weight_fraction real,
t06_odd_a15_no_count real,
t06_odd_a15_no_count_fraction real,
t06_odd_a15_no_debiased real,
t06_odd_a15_no_weight real,
t06_odd_a15_no_weight_fraction real,
t06_odd_count real,
t06_odd_weight real,
t07_rounded_a16_completely_round_count real,
t07_rounded_a16_completely_round_count_fraction real,
t07_rounded_a16_completely_round_debiased real,
t07_rounded_a16_completely_round_weight real,
t07_rounded_a16_completely_round_weight_fraction real,
t07_rounded_a17_in_between_count real,
t07_rounded_a17_in_between_count_fraction real,
t07_rounded_a17_in_between_debiased real,
t07_rounded_a17_in_between_weight real,
t07_rounded_a17_in_between_weight_fraction real,
t07_rounded_a18_cigar_shaped_count real,
t07_rounded_a18_cigar_shaped_count_fraction real,
t07_rounded_a18_cigar_shaped_debiased real,
t07_rounded_a18_cigar_shaped_weight real,
t07_rounded_a18_cigar_shaped_weight_fraction real,
t07_rounded_count real,
t07_rounded_weight real,
t09_bulge_shape_a25_rounded_count real,
t09_bulge_shape_a25_rounded_count_fraction real,
t09_bulge_shape_a25_rounded_debiased real,
t09_bulge_shape_a25_rounded_weight real,
t09_bulge_shape_a25_rounded_weight_fraction real,
t09_bulge_shape_a26_boxy_count real,
t09_bulge_shape_a26_boxy_count_fraction real,
t09_bulge_shape_a26_boxy_debiased real,
t09_bulge_shape_a26_boxy_weight real,
t09_bulge_shape_a26_boxy_weight_fraction real,
t09_bulge_shape_a27_no_bulge_count real,
t09_bulge_shape_a27_no_bulge_count_fraction real,
t09_bulge_shape_a27_no_bulge_debiased real,
t09_bulge_shape_a27_no_bulge_weight real,
t09_bulge_shape_a27_no_bulge_weight_fraction real,
t09_bulge_shape_count real,
t09_bulge_shape_weight real,
t10_arms_winding_a28_tight_count real,
t10_arms_winding_a28_tight_count_fraction real,
t10_arms_winding_a28_tight_debiased real,
t10_arms_winding_a28_tight_weight real,
t10_arms_winding_a28_tight_weight_fraction real,
t10_arms_winding_a29_medium_count real,
t10_arms_winding_a29_medium_count_fraction real,
t10_arms_winding_a29_medium_debiased real,
t10_arms_winding_a29_medium_weight real,
t10_arms_winding_a29_medium_weight_fraction real,
t10_arms_winding_a30_loose_count real,
t10_arms_winding_a30_loose_count_fraction real,
t10_arms_winding_a30_loose_debiased real,
t10_arms_winding_a30_loose_weight real,
t10_arms_winding_a30_loose_weight_fraction real,
t10_arms_winding_count real,
t10_arms_winding_weight real,
t11_arms_number_a31_1_count real,
t11_arms_number_a31_1_count_fraction real,
t11_arms_number_a31_1_debiased real,
t11_arms_number_a31_1_weight real,
t11_arms_number_a31_1_weight_fraction real,
t11_arms_number_a32_2_count real,
t11_arms_number_a32_2_count_fraction real,
t11_arms_number_a32_2_debiased real,
t11_arms_number_a32_2_weight real,
t11_arms_number_a32_2_weight_fraction real,
t11_arms_number_a33_3_count real,
t11_arms_number_a33_3_count_fraction real,
t11_arms_number_a33_3_debiased real,
t11_arms_number_a33_3_weight real,
t11_arms_number_a33_3_weight_fraction real,
t11_arms_number_a34_4_count real,
t11_arms_number_a34_4_count_fraction real,
t11_arms_number_a34_4_debiased real,
t11_arms_number_a34_4_weight real,
t11_arms_number_a34_4_weight_fraction real,
t11_arms_number_a36_more_than_4_count real,
t11_arms_number_a36_more_than_4_count_fraction real,
t11_arms_number_a36_more_than_4_debiased real,
t11_arms_number_a36_more_than_4_weight real,
t11_arms_number_a36_more_than_4_weight_fraction real,
t11_arms_number_a37_cant_tell_count real,
t11_arms_number_a37_cant_tell_count_fraction real,
t11_arms_number_a37_cant_tell_debiased real,
t11_arms_number_a37_cant_tell_weight real,
t11_arms_number_a37_cant_tell_weight_fraction real,
t11_arms_number_count real,
t11_arms_number_weight real,
Primary Key (nsa_id)
);