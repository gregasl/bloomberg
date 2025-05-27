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
    contract_ticker nchar(4) NOT NULL,
    sec_type nvarchar(8) NOT NULL,
    sec_description nvarchar(32) NOT NULL,
    first_deliv_date date not null,
    last_deliv_date date not null,
    ctd_cusip nchar(9) not null,
    deliver_cusip_list nvarchar(4096) NOT NULL
)
go 

CREATE NONCLUSTERED INDEX bus_date_indx on dbo.bloomberg_fut_sec_data(business_date)
go

CREATE NONCLUSTERED INDEX ticker_bus_date on dbo.bloomberg_fut_sec_data(contract_ticker, business_date)
go