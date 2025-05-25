use playdb
go

if exists(select 1 from INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'bloomberg_mbs_sec_data')
begin
    drop table bloomberg_mbs_sec_data
end
go

create table bloomberg_mbs_sec_data(
    business_date date NOT NULL,
    cusip char(9) NOT NULL,
    ISIN char(12) NOT NULL,
    issuer varchar(16) NOT NULL,
    sec_type varchar(8) NOT NULL,
    ticker char(2) not null,
    sec_description varchar(32) NOT NULL,
    maturity_date date NOT NULL,
    issue_date date NOT NULL,
    amt_outstanding money NOT NULL,
    factor numeric(9,7) NOT NULL default 1.0,
    cpr_3mo numeric(10,6) NOT NULL,
    wal numeric(10,6) NOT NULL,
    dur_adj_mid numeric(8,5) NOT NULL,
    dur_adj_oas_mid numeric(8,5) NOT NULL,
    key_rate_dur_3mo numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_6mo numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_1yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_2yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_3yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_4yr numeric(8,5) NOT NULl default 0.0,
    key_rate_dur_5yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_6yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_7yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_8yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_9yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_10yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_15yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_20yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_25yr numeric(8,5) NOT NULL default 0.0,
    key_rate_dur_30yr numeric(8,5) NOT NULL default 0.0
)
go 