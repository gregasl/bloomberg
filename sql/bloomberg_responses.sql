use playdb
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_responses' AND xtype='U')
begin
    drop table bloomberg_responses
end
go


CREATE TABLE bloomberg_responses (
    response_id NVARCHAR(50) PRIMARY KEY,
    request_id NVARCHAR(50) NOT NULL,
    identifier NVARCHAR(100) NOT NULL,
    response_key NVARCHAR(200),
    status_code INT,
    response_data NTEXT,
    snapshot_timestamp NVARCHAR(50),
    error_message NTEXT,
    received_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (request_id) REFERENCES bloomberg_requests(request_id)
)
go

CREATE NONCLUSTERED INDEX request_id_indx on dbo.bloomberg_responses(request_id)
go