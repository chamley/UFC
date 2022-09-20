create table fight_source
(
    fight_key_nat text,
    red_fighter_name text,
    red_fighter_id text,
    blue_fighter_name text,
    blue_fighter_id text,
    winner_fighter_name text,
    winner_fighter_id text,
    details text,
    final_round numeric,
    final_round_duration time,
    method text,
    referee text,
    round_format text,
    weight_class text,
    fight_date date,
    is_title_fight int,
    wmma int,
    wc text,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)