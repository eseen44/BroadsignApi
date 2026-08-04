====================================================================================================
  BroadsignApi -- nowy prefiks broadsign/gold (przeniesienie z plaskiej sciezki)
====================================================================================================

-- dim_campaign: 1,246 wierszy, 18 kolumn, 0.1 MB
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.dim_campaign (
    campaign_id               bigint,         -- parquet: int64
    campaign_name             string,         -- parquet: string
    campaign_status           bigint,         -- parquet: int64
    advertiser                string,         -- parquet: string
    client_id                 bigint,         -- parquet: int64
    client_name               string,         -- parquet: string
    campaign_price            double,         -- parquet: double
    campaign_suggested_price  double,         -- parquet: double
    campaign_discount         double,         -- parquet: double
    campaign_start            timestamp,      -- parquet: timestamp[ns]
    campaign_end              timestamp,      -- parquet: timestamp[ns]
    owner_email               string,         -- parquet: string
    owner_user_id             bigint,         -- parquet: int64
    campaign_owner_name       string,         -- parquet: string
    contract_id               string,         -- parquet: string
    contract_number           string,         -- parquet: string
    is_serwisowy              tinyint,        -- parquet: int8
    _gold_at                  string          -- parquet: string
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/broadsign/gold/dim_campaign/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_campaign_period: 14,175 wierszy, 8 kolumn, 0.1 MB
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.dim_campaign_period (
    campaign_id           bigint,         -- parquet: int64
    grain                 string,         -- parquet: string
    year_month            string,         -- parquet: string
    year_month_dt         timestamp,      -- parquet: timestamp[ns]
    year_week             string,         -- parquet: string
    year_week_dt          timestamp,      -- parquet: timestamp[ns]
    week_parent_month_dt  timestamp,      -- parquet: timestamp[ns]
    _gold_at              string          -- parquet: string
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/broadsign/gold/dim_campaign_period/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_content: 2,822 wierszy, 4 kolumn, 0.1 MB
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.dim_content (
    content_id    bigint,         -- parquet: int64
    content_name  string,         -- parquet: string
    domain_id     bigint,         -- parquet: int64
    _gold_at      string          -- parquet: string
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/broadsign/gold/dim_content/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_date: 1,095 wierszy, 18 kolumn, 0.0 MB
-- UWAGA: kolumny z wielkimi literami ['Date', 'Year', 'Month Number', 'Month Name', 'Short Month', 'Quarter', 'Year-Month', 'Weekday Name', 'Short Weekday', 'Weekday Number', 'ISO Year', 'ISO Week', 'ISO Week2', 'ISO Week3', 'YearWeek Index'] -> Athena zwroci je malymi literami
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.dim_date (
    date            string,         -- parquet: string
    date_key        string,         -- parquet: string
    year            int,            -- parquet: int32
    month number    int,            -- parquet: int32
    month name      string,         -- parquet: string
    short month     string,         -- parquet: string
    quarter         string,         -- parquet: string
    year-month      string,         -- parquet: string
    weekday name    string,         -- parquet: string
    short weekday   string,         -- parquet: string
    weekday number  int,            -- parquet: int32
    iso year        int,            -- parquet: int32
    iso week        int,            -- parquet: int32
    iso week2       string,         -- parquet: string
    iso week3       string,         -- parquet: string
    yearweek index  int,            -- parquet: int32
    is_weekend      boolean,        -- parquet: bool
    _gold_at        string          -- parquet: string
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/broadsign/gold/dim_date/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_line_item: 3,977 wierszy, 34 kolumn, 0.3 MB
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.dim_line_item (
    line_item_id               bigint,         -- parquet: int64
    campaign_id                bigint,         -- parquet: int64
    line_item_name             string,         -- parquet: string
    type_of_buy                string,         -- parquet: string
    slot_duration              double,         -- parquet: double
    screen_count               bigint,         -- parquet: int64
    group_count                bigint,         -- parquet: int64
    status_id                  double,         -- parquet: double
    status_name                string,         -- parquet: string
    is_preemptible             string,         -- parquet: string
    line_price                 string,         -- parquet: string
    line_suggested_price       string,         -- parquet: string
    buy_saturation             double,         -- parquet: double
    buy_bs_saturation          double,         -- parquet: double
    buy_sov                    double,         -- parquet: double
    buy_budget                 double,         -- parquet: double
    perf_expected_repetitions  double,         -- parquet: double
    perf_actual_repetitions    double,         -- parquet: double
    perf_expected_impressions  double,         -- parquet: double
    perf_actual_impressions    double,         -- parquet: double
    broadsign_status           string,         -- parquet: string
    line_start                 string,         -- parquet: string
    line_end                   string,         -- parquet: string
    line_days                  bigint,         -- parquet: int64
    reservation_id             bigint,         -- parquet: int64
    booking_state              double,         -- parquet: double
    reservation_name           string,         -- parquet: string
    reservation_start          string,         -- parquet: string
    reservation_end            string,         -- parquet: string
    reservation_state          double,         -- parquet: double
    saturation                 double,         -- parquet: double
    duration_msec              double,         -- parquet: double
    active                     boolean,        -- parquet: bool
    _gold_at                   string          -- parquet: string
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/broadsign/gold/dim_line_item/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- dim_player: 324 wierszy, 12 kolumn, 0.0 MB
-- UWAGA: kolumny z wielkimi literami ['Lokalizacja'] -> Athena zwroci je malymi literami
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.dim_player (
    play_log_player_id  bigint,         -- parquet: int64
    player_id           bigint,         -- parquet: int64
    player_name         string,         -- parquet: string
    hostname            string,         -- parquet: string
    display_unit_id     bigint,         -- parquet: int64
    display_unit_name   string,         -- parquet: string
    du_address          string,         -- parquet: string
    timezone            string,         -- parquet: string
    nscreens            bigint,         -- parquet: int64
    lokalizacja         string,         -- parquet: string
    is_test             boolean,        -- parquet: bool
    _gold_at            string          -- parquet: string
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/broadsign/gold/dim_player/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- fact_campaign_budget: 2,029,451 wierszy, 13 kolumn, 0.5 MB
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.fact_campaign_budget (
    campaign_id                 bigint,         -- parquet: int64
    line_item_id                bigint,         -- parquet: int64
    reservation_id              bigint,         -- parquet: int64
    play_log_player_id          bigint,         -- parquet: int64
    date_key                    string,         -- parquet: string
    daily_cost_line             double,         -- parquet: double
    daily_expected_repetitions  double,         -- parquet: double
    daily_actual_repetitions    double,         -- parquet: double
    duration_expected_sec       double,         -- parquet: double
    duration_actual_sec         double,         -- parquet: double
    n_days                      bigint,         -- parquet: int64
    n_players                   bigint,         -- parquet: int64
    _gold_at                    string          -- parquet: string
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/broadsign/gold/fact_campaign_budget/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- fact_health: 86,422 wierszy, 9 kolumn, 0.5 MB
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.fact_health (
    date_key              timestamp,      -- parquet: timestamp[ns]
    play_log_player_id    bigint,         -- parquet: int64
    n_campaigns           bigint,         -- parquet: int64
    n_line_items          bigint,         -- parquet: int64
    expected_impressions  double,         -- parquet: double
    actual_impressions    double,         -- parquet: double
    has_emission          boolean,        -- parquet: bool
    incident              boolean,        -- parquet: bool
    _gold_at              string          -- parquet: string
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/broadsign/gold/fact_health/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- fact_play_logs: 13,270,307 wierszy, 15 kolumn, 55.4 MB
-- UWAGA: kolumny z wielkimi literami ['Impresje', 'Duration'] -> Athena zwroci je malymi literami
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.fact_play_logs (
    date_key            timestamp,      -- parquet: timestamp[ns]
    play_log_player_id  bigint,         -- parquet: int64
    frame_id            bigint,         -- parquet: int64
    display_unit_id     bigint,         -- parquet: int64
    reservation_id      bigint,         -- parquet: int64
    content_id          bigint,         -- parquet: int64
    timeslot            bigint,         -- parquet: int64
    contract_id         string,         -- parquet: string
    emisje              bigint,         -- parquet: int64
    impresje            bigint,         -- parquet: int64
    duration            bigint,         -- parquet: int64
    line_item_id        bigint,         -- parquet: int64
    campaign_id         bigint,         -- parquet: int64
    is_serwisowy        tinyint,        -- parquet: int8
    _gold_at            string          -- parquet: string
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/broadsign/gold/fact_play_logs/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
