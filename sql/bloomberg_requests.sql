use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_requests' AND xtype='U')
begin
    drop table bloomberg_requests
end
go

/*  Create requests table */
CREATE TABLE bloomberg_requests (
    request_id NVARCHAR(50) PRIMARY KEY,
    identifier NVARCHAR(100) NOT NULL,
    request_name NVARCHAR(200),
    request_title NVARCHAR(500),
    request_payload NTEXT NOT NULL,
    status NVARCHAR(20) DEFAULT 'pending',
    priority INT DEFAULT 1 NOT NULL,
    retry_count INT DEFAULT 0 NOT NULL,
    max_retries INT DEFAULT 3 NOT NULL,
    submitted_at DATETIME2 NULL,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
)
go