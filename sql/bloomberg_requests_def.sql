use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_requests' AND xtype='U')
begin
    drop table bloomberg_requests_def
end
go

/*  Create requests table */
CREATE TABLE bloomberg_requests_def (
    request_name NVARCHAR(64) PRIMARY KEY,
    request_title NVARCHAR(128) NOT NULL,
    priority INT DEFAULT 4 NOT NULL,
    save_table NVARCHAR(128) NULL,
    save_file NVARCHAR(512) NULL,
    retry_wait_sec INT DEFAULT 120 NOT NULL,
    max_request_retries INT DEFAULT 5 NOT NULL,
    response_poll_wait_sec INT DEFAULT 30 NOT NULL,
    max_response_polls INT DEFAULT 120 NOT NULL,
    ts DATETIME2 DEFAULT GETDATE()
)
go


