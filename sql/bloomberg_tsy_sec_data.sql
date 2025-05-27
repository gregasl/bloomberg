use playdb
go

if exists(select 1 from INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'bloomberg_tsy_sec_data')
begin
    drop table bloomberg_tsy_sec_data
end
go

create table bloomberg_tsy_sec_data(
    
    business_date date NOT NULL,
    cusip char(9) NOT NULL,
    ISIN char(12) NOT NULL,
    sec_type varchar(8) NOT NULL,
    sec_description varchar(32) NOT NULL,
    maturity_date date NOT NULL,
    issue_date date NOT NULL,
    pricing_source varchar(12) NOT NULL default 'BBG',
    inflation_linked bit not null default 0,
    amt_outstanding money NOT NULL,
    amt_central_bank money NOT NULL,
    factor numeric(9,7) NOT NULL default 1.0,
    index_ratio numeric(9, 7) NULL,
    spread_to_spline numeric(12,6) NULL,
    dur_adj_bid numeric(8,5) NOT NULL,
    cnvx_bid numeric(8,5) NOT NULL,                                                                                         
    yld_cnv_bin numeric(8, 5) NOT NULL,
    key_rate_3mo numeric(8,5) NOT NULL default 0.0,
    key_rate_6mo numeric(8,5) NOT NULL default 0.0,
    key_rate_1yr numeric(8,5) NOT NULL default 0.0,
    key_rate_2yr numeric(8,5) NOT NULL default 0.0,
    key_rate_3yr numeric(8,5) NOT NULL default 0.0,
    key_rate_4yr numeric(8,5) NOT NULl default 0.0,
    key_rate_5yr numeric(8,5) NOT NULL default 0.0,
    key_rate_6yr numeric(8,5) NOT NULL default 0.0,
    key_rate_7yr numeric(8,5) NOT NULL default 0.0,
    key_rate_8yr numeric(8,5) NOT NULL default 0.0,
    key_rate_9yr numeric(8,5) NOT NULL default 0.0,
    key_rate_10yr numeric(8,5) NOT NULL default 0.0,
    key_rate_15yr numeric(8,5) NOT NULL default 0.0,
    key_rate_20yr numeric(8,5) NOT NULL default 0.0,
    key_rate_25yr numeric(8,5) NOT NULL default 0.0,
    key_rate_30yr numeric(8,5) NOT NULL default 0.0,
    oas_key_rate_3mo NUMERIC(8,5) NULL,
    oas_key_rate_6mo NUMERIC(8,5) NULL,
    oas_key_rate_1yr NUMERIC(8,5) NULL,
    oas_key_rate_2yr NUMERIC(8,5) NULL,
    oas_key_rate_3yr NUMERIC(8,5) NULL,
    oas_key_rate_5yr NUMERIC(8,5) NULL,
    oas_key_rate_7yr NUMERIC(8,5) NULL,
    oas_key_rate_10yr NUMERIC(8,5) NULL,
    oas_key_rate_20yr NUMERIC(8,5) NULL,
    oas_key_rate_30yr NUMERIC(8,5) NULL
)
go 