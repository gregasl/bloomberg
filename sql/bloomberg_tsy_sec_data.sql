use <database>
go

if exists(select 1 from INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'bloomberg_tsy_sec_data')
begin
    drop table bloomberg_tsy_sec_data
end
go

create table bloomberg_tsy_sec_data(
    id BIGINT IDENTITY(1,1) PRIMARY key,
    business_date date NOT NULL,
    cusip nchar(9) NOT NULL,
    ISIN nchar(12) NOT NULL,
    sec_type nvarchar(32) NOT NULL,
    sec_description nvarchar(64) NOT NULL,
    maturity_date date NOT NULL,
    issue_date date NOT NULL,
    pricing_source nvarchar(12) NOT NULL default 'BBG',
    inflation_linked bit not null default 0,
    amt_outstanding money NOT NULL,
    amt_central_bank money NOT NULL,
    factor numeric(9,7) NOT NULL default 1.0,
    index_ratio numeric(9, 7) NULL,
    spread_to_spline numeric(12,6) NULL,
    dur_adj_bid numeric(8,5) NULL,
    cnvx_bid numeric(8,5) NULL, 
    px_bid numeric(12,6) NULL,
    px_disc_bid numeric(12,6) NULL,
    quote_typ nvarchar(24) NULL,
    yld_cnv_bin numeric(8, 5) NULL,
    key_rate_3mo numeric(8,5) NULL,
    key_rate_6mo numeric(8,5) NULL,
    key_rate_1yr numeric(8,5) NULL,
    key_rate_2yr numeric(8,5) NULL,
    key_rate_3yr numeric(8,5) NULL,
    key_rate_4yr numeric(8,5) NULL,
    key_rate_5yr numeric(8,5) NULL,
    key_rate_6yr numeric(8,5) NULL,
    key_rate_7yr numeric(8,5) NULL,
    key_rate_8yr numeric(8,5) NULL,
    key_rate_9yr numeric(8,5) NULL,
    key_rate_10yr numeric(8,5) NULL,
    key_rate_15yr numeric(8,5) NULL,
    key_rate_20yr numeric(8,5) NULL,
    key_rate_25yr numeric(8,5) NULL,
    key_rate_30yr numeric(8,5)  NULL,
    oas_key_rate_3mo NUMERIC(8,5) NULL,
    oas_key_rate_6mo NUMERIC(8,5) NULL,
    oas_key_rate_1yr NUMERIC(8,5) NULL,
    oas_key_rate_2yr NUMERIC(8,5) NULL,
    oas_key_rate_3yr NUMERIC(8,5) NULL,
    oas_key_rate_5yr NUMERIC(8,5) NULL,
    oas_key_rate_7yr NUMERIC(8,5) NULL,
    oas_key_rate_10yr NUMERIC(8,5) NULL,
    oas_key_rate_20yr NUMERIC(8,5) NULL,
    oas_key_rate_30yr NUMERIC(8,5) NULL,
    ts DATETIME2 DEFAULT GETDATE()
)
go 

CREATE NONCLUSTERED INDEX bus_date_indx on dbo.bloomberg_tsy_sec_data(business_date)
go

CREATE NONCLUSTERED INDEX cusip_bus_date on dbo.bloomberg_tsy_sec_data(cusip, business_date)
go
