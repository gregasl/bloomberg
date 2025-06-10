use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_requests' AND xtype='U')
begin
    drop table bloomberg_requests
end
go

/*  Create requests table */
CREATE TABLE bloomberg_requests (
    request_id NVARCHAR(50) PRIMARY key,
    identifier NVARCHAR(100) NOT NULL,
    request_name NVARCHAR(200),
    request_title NVARCHAR(500),
    request_payload NTEXT NOT NULL,
    status NVARCHAR(20) DEFAULT 'pending' NOT NULL  CONSTRAINT status_check CHECK (archived_typ in ('pending', 'processing', 'submitted', 'complete')),
    priority INT DEFAULT 1 NOT NULL,
    request_retry_count INT DEFAULT 0 NOT NULL,
    max_request_retries INT DEFAULT 3 NOT NULL,
    submitted_at DATETIME2 NULL,
    response_poll_count INT DEFAULT 0 NOT NULL,
    max_response_polls INT DEFAULT 120 NOT NULL,
    last_poll_at DATETIME2 DEFAULT GETDATE() NULL,
    created_at DATETIME2 DEFAULT GETDATE() NOT NULL,
    completed_at DATETIME2 DEFAULT GETDATE() NULL,
    updated_at DATETIME2 DEFAULT GETDATE() NULL,
    ts DATETIME2 DEFAULT GETDATE()
)
go


