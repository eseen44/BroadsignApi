====================================================================================================
  SWAT -- ims_total, campaign_adjustment, sales
====================================================================================================

-- campaign_adjustment: 6,811 wierszy, 9 kolumn, 0.2 MB
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.campaign_adjustment (
    camp_number      bigint,         -- parquet: int64
    sales_amount     double,         -- parquet: double
    original_amount  double,         -- parquet: double
    ania_amount      double,         -- parquet: double
    beer_fee         boolean,        -- parquet: bool
    use_original     boolean,        -- parquet: bool
    net_final        double,         -- parquet: double
    delta_vs_ania    double,         -- parquet: double
    delta_pct        double          -- parquet: double
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/swat/campaign_adjustment/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ims_total: 9,045 wierszy, 37 kolumn, 1.2 MB
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.ims_total (
    panel_number               bigint,         -- parquet: int64
    type                       string,         -- parquet: string
    environment                bigint,         -- parquet: int64
    illumination               bigint,         -- parquet: int64
    motion                     string,         -- parquet: string
    width_m                    double,         -- parquet: double
    height_m                   double,         -- parquet: double
    address                    string,         -- parquet: string
    coordinate_x               double,         -- parquet: double
    coordinate_y               double,         -- parquet: double
    presentation_coordinate_x  double,         -- parquet: double
    presentation_coordinate_y  double,         -- parquet: double
    angle_to_map_north         bigint,         -- parquet: int64
    max_visibility_distance_m  bigint,         -- parquet: int64
    zip_code                   string,         -- parquet: string
    municipality_name          string,         -- parquet: string
    municipality_code          bigint,         -- parquet: int64
    agglomeration              string,         -- parquet: string
    status                     bigint,         -- parquet: int64
    status_editor              string,         -- parquet: string
    date                       timestamp,      -- parquet: timestamp[ns]
    availability               string,         -- parquet: string
    mid                        bigint,         -- parquet: int64
    release                    string,         -- parquet: string
    vac                        double,         -- parquet: double
    vac_category               string,         -- parquet: string
    vac_vehicles               double,         -- parquet: double
    vac_pedestrians            double,         -- parquet: double
    vac_public_transport       double,         -- parquet: double
    rots                       double,         -- parquet: double
    rots_vehicles              double,         -- parquet: double
    rots_pedestrians           double,         -- parquet: double
    rots_public_transport      double,         -- parquet: double
    grp                        double,         -- parquet: double
    grp_vehicles               double,         -- parquet: double
    grp_pedestrians            double,         -- parquet: double
    grp_public_transport       double          -- parquet: double
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/swat/ims_total/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- sales: 513,963 wierszy, 40 kolumn, 20.4 MB
-- UWAGA: kolumny z wielkimi literami ['Id', 'CampId', 'FormatId', 'PanelId', 'FinalClientId', 'CustomerId', 'PayerId', 'InvoiceReceiverId', 'PlannerId', 'AccountId', 'CompanyId', 'Amount', 'OriginalAmount', 'CorrectionAmount', 'OriginalCorrectionAmount', 'CorrectionPanelsNo', 'CorrectionTotalAmount', 'CategoryId', 'MaterialId', 'MaterialGroupId', 'CampaignType', 'PriceList', 'PSP', 'PSP_SYS', 'PositionNo', 'CorrectionPositionNo', 'PanelsNo', 'BeerFee', 'DistributionChannel', 'TimeKey', 'NetAmount', 'OriginalNetAmount', 'NetPanelsNo', 'TotalNetAmount', 'PanelsCount', 'LineType', 'PanelsCountS2', 'CampNumber', 'Blok'] -> Athena zwroci je malymi literami
CREATE EXTERNAL TABLE data_warehouse_non_samm_prod.sales (
    id                        bigint,         -- parquet: int64
    campid                    bigint,         -- parquet: int64
    formatid                  bigint,         -- parquet: int64
    panelid                   bigint,         -- parquet: int64
    finalclientid             bigint,         -- parquet: int64
    customerid                bigint,         -- parquet: int64
    payerid                   bigint,         -- parquet: int64
    invoicereceiverid         bigint,         -- parquet: int64
    plannerid                 bigint,         -- parquet: int64
    accountid                 bigint,         -- parquet: int64
    companyid                 double,         -- parquet: double
    amount                    double,         -- parquet: double
    originalamount            double,         -- parquet: double
    correctionamount          double,         -- parquet: double
    originalcorrectionamount  double,         -- parquet: double
    correctionpanelsno        double,         -- parquet: double
    correctiontotalamount     double,         -- parquet: double
    categoryid                bigint,         -- parquet: int64
    materialid                bigint,         -- parquet: int64
    materialgroupid           bigint,         -- parquet: int64
    campaigntype              string,         -- parquet: string
    pricelist                 string,         -- parquet: string
    psp                       string,         -- parquet: string
    psp_sys                   string,         -- parquet: string
    positionno                bigint,         -- parquet: int64
    correctionpositionno      double,         -- parquet: double
    panelsno                  bigint,         -- parquet: int64
    beerfee                   string,         -- parquet: string
    distributionchannel       double,         -- parquet: double
    timekey                   timestamp,      -- parquet: timestamp[ns]
    netamount                 double,         -- parquet: double
    originalnetamount         double,         -- parquet: double
    netpanelsno               bigint,         -- parquet: int64
    totalnetamount            double,         -- parquet: double
    panelscount               bigint,         -- parquet: int64
    linetype                  bigint,         -- parquet: int64
    panelscounts2             bigint,         -- parquet: int64
    campnumber                bigint,         -- parquet: int64
    blok                      string,         -- parquet: string
    net_final                 double          -- parquet: double
)
STORED AS PARQUET
LOCATION 's3://stroeer-samm-data-warehouse-non-samm-prod/swat/sales/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
