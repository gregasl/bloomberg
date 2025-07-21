use <database>
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_requests_archive_trig' AND xtype='TR')
begin
    drop trigger bloomberg_requests_archive_trig
end
go

/*  Create requests table */
create trigger bloomberg_requests_archive_trig on  bloomberg_requests
    AFTER INSERT, UPDATE, DELETE
    NOT FOR REPLICATION
    as 
    BEGIN
        DECLARE @archive_type CHAR = 'I'

        IF exists(select * from inserted)
        BEGIN
            UPDATE br
                SET ts = GETDATE() 
            FROM dbo.bloomberg_requests AS br
            INNER JOIN Inserted AS i
                ON br.request_id = i.request_id;

          IF (exists (select * from deleted))
            BEGIN
                set @archive_type = 'U'
            END

        INSERT INTO bloomberg_requests_archive (
            request_id,
            identifier,
            name,
            title,
            status,
            request_error,
            priority,
            response_id,
            response,
            request_retry_count,
            max_request_retries,
            retry_wait_sec,
            response_poll_count,
            max_response_polls,
            response_poll_wait_sec,
            last_poll_at,
            created_at,
            submitted_at,
            completed_at,
            updated_at,
            ts,
            archive_type
        ) select 
            i.request_id,
            i.identifier,
            i.name,
            i.title,
            i.status,
            i.request_error,
            i.priority,
            i.response_id,
            i.response,
            i.request_retry_count,
            i.max_request_retries,
            i.retry_wait_sec,
            i.response_poll_count,
            i.max_response_polls,
            i.response_poll_wait_sec,
            i.last_poll_at,
            i.created_at,
            i.submitted_at,
            i.completed_at,
            i.updated_at,
            GETDATE(),
            @archive_type from inserted i
        END
      else IF exists(select * from DELETED)
        BEGIN
        INSERT INTO bloomberg_requests_archive (
            request_id,
            identifier,
            name,
            title,
            status,
            request_error,
            priority,
            response_id,
            response,
            request_retry_count,
            max_request_retries,
            retry_wait_sec,
            response_poll_count,
            max_response_polls,
            response_poll_wait_sec,
            last_poll_at,
            created_at,
            submitted_at,
            completed_at,
            updated_at,
            ts,
            archive_type
        ) select 
            d.request_id,
            d.identifier,
            d.name,
            d.title,
            d.status,
            d.request_error,
            d.priority,
            d.response_id,
            d.response,
            d.request_retry_count,
            d.max_request_retries,
            d.retry_wait_sec,
            d.response_poll_count,
            d.max_response_polls,
            d.response_poll_wait_sec,
            d.last_poll_at,
            d.created_at,
            d.submitted_at,
            d.completed_at,
            d.updated_at,
            GETDATE(),
            'D' from deleted d
        END
    END
go

