use <database>
go

IF EXISTS (SELECT * FROM sysobjects WHERE name='bloomberg_process_archive_trig' AND xtype='TR')
begin
    drop trigger bloomberg_process_archive_trig
end
go

/*  Create requests table */
create trigger bloomberg_process_archive_trig on  bloomberg_process_status
    AFTER INSERT, UPDATE, DELETE
    NOT FOR REPLICATION
    as 
    BEGIN
        DECLARE @archive_type CHAR = 'I'

        IF exists(select * from inserted)
        BEGIN
            UPDATE br
                SET ts = GETDATE() 
            FROM dbo.bloomberg_process_status AS br
            INNER JOIN Inserted AS i
                ON br.request_id = i.request_id;

          IF (exists (select * from deleted))
            BEGIN
                set @archive_type = 'U'
            END

        INSERT INTO bloomberg_process_status_archive (
            request_id, 
            identifier,
            name,
            process_type,
            processed_status, 
            process_error,
            ts,
            archive_type
        ) select 
            i.request_id,
            i.identifier,
            i.name,
            i.process_type,
            i.processed_status,
            i.process_error,
            GETDATE(),
            @archive_type from inserted i
        END
      else IF exists(select * from DELETED)
        BEGIN
        INSERT INTO bloomberg_process_status_archive (
             request_id, 
             identifier,
             name, 
             process_type,
             processed_status, 
             process_error,
             ts,
             archive_type
         ) select 
             d.request_id,
             d.identifier,
             d.name,
             d.process_type,
             d.processed_status,
             d.process_error,
             GETDATE(),
            'D' 
            from deleted d
        END
    END
go

