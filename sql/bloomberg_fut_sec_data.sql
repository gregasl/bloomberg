use <database>
go

if exists(select 1 from INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'bloomberg_fut_sec_data')
begin
    drop table bloomberg_fut_sec_data
end
go

create table bloomberg_fut_sec_data(
    business_date date NOT NULL,
    ticker nchar(8) NOT NULL,
    first_deliv_date date not null,
    last_deliv_date date not null,
    ctd_cusip nchar(9) not null,
    deliver_cusip_list nvarchar(4000) NOT NULL,
    ts DATETIME2 DEFAULT GETDATE()
)
go 

CREATE NONCLUSTERED INDEX bus_date_indx on dbo.bloomberg_fut_sec_data(business_date)
go

CREATE NONCLUSTERED INDEX ticker_bus_date on dbo.bloomberg_fut_sec_data(ticker, business_date)
go
