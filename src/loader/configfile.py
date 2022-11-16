config_settings = {
    # general settings
    "REGION_NAME": "us-east-1",
    # S3 bucket names
    "STAGE_LAYER_ONE": "ufc-big-data",
    "STAGE_LAYER_TWO": "ufc-big-data-2",
    "UFC_META_FILES_LOCATION": "ufc-meta-files",
    "LOAD_MANIFEST_FOLDER": "load_manifests",
    "E1_CSV_OPT_DATE_FOLDER_PATH": "e1-dates",
    # roles
    "REDSHIFT_S3_READ_IAM_ROLE": (
        "arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess"
    ),
    # database table names
    "UFCSTATS_ROUND_SOURCE_TABLE_NAME": "round_source",
    "UFCSTATS_FIGHT_SOURCE_TABLE_NAME": "fight_source",
    "UFCSTATS_ROUND_SOURCE_SCHEMA": "kd, ss_l, ss_a, ts_l, ts_a, td_l, td_a, sub_a, rev, ctrl, ss_l_h, ss_a_h, ss_l_b, ss_a_b, ss_l_l, ss_a_l, ss_l_dist, ss_a_dist, ss_l_cl, ss_a_cl, ss_l_gr, ss_a_gr, fighter_name, fighter_id, fight_key_nat, round_num",
    "UFCSTATS_FIGHT_SOURCE_SCHEMA": "fight_key_nat, red_fighter_name, red_fighter_id, blue_fighter_name, blue_fighter_id, winner_fighter_name, winner_fighter_id, details, final_round, final_round_duration, method, referee, round_format, weight_class, fight_date, is_title_fight, wmma, wc",
}
