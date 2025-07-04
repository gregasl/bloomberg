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
    name NVARCHAR(64) NOT NULL,
    title NVARCHAR(64) NOT NULL,
    payload NVARCHAR(4000)NULL,
    status NVARCHAR(20) DEFAULT 'pending' NOT NULL  CONSTRAINT bbg_requests_status_check CHECK (status in ('pending', 'processing', 'submitted', 'completed')),
    priority INT DEFAULT 1 NOT NULL,
    response_id  NVARCHAR(50) NULL,
    response NVARCHAR(128) NULL,
    response_status NVARCHAR(20) DEFAULT 'pending' NOT NULL  CONSTRAINT bbg_response_status_check CHECK (response_status in ('pending', 'error', 'success')),
    request_retry_count INT DEFAULT 0 NOT NULL,
    max_request_retries INT DEFAULT 5 NOT NULL,
    retry_wait_sec INT DEFAULT 120 NOT NULL,
    response_poll_count INT DEFAULT 0 NOT NULL,
    max_response_polls INT DEFAULT 120 NOT NULL,
    response_poll_wait_sec INT DEFAULT 30 NOT NULL,
    last_poll_at DATETIME2 NULL,
    created_at DATETIME2 DEFAULT GETDATE() NOT NULL,
    submitted_at DATETIME2 NULL,
    completed_at DATETIME2 NULL,
    updated_at DATETIME2  NULL,
    ts DATETIME2 DEFAULT GETDATE()
)
go

CREATE NONCLUSTERED INDEX bloomberg_requests_request_id_indx on dbo.bloomberg_requests(request_id, ts)
go


