use <database>
go

if exists(select 1 from INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'bloomberg_mbs_sec_data')
begin
    drop table bloomberg_mbs_sec_data
end
go

create table bloomberg_mbs_sec_data(
    business_date date NOT NULL,
    cusip nchar(9) NOT NULL,
    issuer nvarchar(16) NOT NULL,
    sec_type nvarchar(16) NOT NULL,
    ticker nchar(2) not null,
    sec_description nvarchar(32) NOT NULL,
    maturity_date date NOT NULL,
    amt_outstanding money NOT NULL,
    factor numeric(9,7) NOT NULL default 1.0,
    cpr_3mo numeric(10,6) NOT NULL,
    wal numeric(10,6) NOT NULL,
    dur_adj_mid numeric(8,5) NULL,
    dur_adj_oas_mid numeric(8,5) NULL,
    px_bid numeric(8,5) NULL,
    px_disc_bid numeric(8,5) NULL,
    key_rate_dur_3mo numeric(8,5) NULL,
    key_rate_dur_6mo numeric(8,5) NULL,
    key_rate_dur_1yr numeric(8,5) NULL,
    key_rate_dur_2yr numeric(8,5) NULL,
    key_rate_dur_3yr numeric(8,5) NULL,
    key_rate_dur_4yr numeric(8,5) NULL,
    key_rate_dur_5yr numeric(8,5) NULL,
    key_rate_dur_6yr numeric(8,5) NULL,
    key_rate_dur_7yr numeric(8,5) NULL,
    key_rate_dur_8yr numeric(8,5) NULL,
    key_rate_dur_9yr numeric(8,5) NULL,
    key_rate_dur_10yr numeric(8,5) NULL,
    key_rate_dur_15yr numeric(8,5) NULL,
    key_rate_dur_20yr numeric(8,5) NULL,
    key_rate_dur_25yr numeric(8,5) NULL,
    key_rate_dur_30yr numeric(8,5) NULL,
    ts DATETIME2 DEFAULT GETDATE()
)
go 

CREATE NONCLUSTERED INDEX bus_date_indx on dbo.bloomberg_mbs_sec_data(business_date)
go

CREATE NONCLUSTERED INDEX cusip_bus_date on dbo.bloomberg_mbs_sec_data(cusip, business_date)
go
