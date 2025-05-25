use playdb
go

if exists(select 1 from INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'bloomberg_fut_sec_data')
begin
    drop table bloomberg_fut_sec_data
end
go

create table bloomberg_fut_sec_data(
    business_date date NOT NULL,
    contract_ticker char(4) NOT NULL,
    sec_type varchar(8) NOT NULL,
    sec_description varchar(32) NOT NULL,
    first_deliv_date date not null,
    last_deliv_date date not null,
    ctd_cusip char(9) not null,
    deliver_cusip_list varchar(4096) NOT NULL
)
go 