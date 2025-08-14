show streams;
select count(*) from customer_table_changes;

--making changes to table for viewing is_current after creating logical view and task
insert into customer values(223136,'Jessica','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut','Cape Verde',current_timestamp());
update customer set FIRST_NAME='Jessica', update_timestamp = current_timestamp()::timestamp_ntz where customer_id=72;
delete from customer where customer_id = 73;

select * from customer_table_changes;

create or replace view v_customer_table_changes as 


WITH
new_inserts AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        street,
        city,
        state,
        country,
        update_timestamp AS start_time,
        '9999-12-31'::timestamp_ntz AS end_time,
        TRUE as is_current,
        'I' as dml_type
    FROM customer_table_changes
    WHERE metadata$action = 'INSERT'
      AND metadata$isupdate = 'FALSE'
),

update_inserts AS (
    SELECT 
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        STREET,
        CITY,
        STATE,
        COUNTRY,
        update_timestamp AS start_time,
        '9999-12-31'::timestamp_ntz AS end_time,
        TRUE AS is_current,
        'I' AS dml_type
    FROM customer_table_changes
    WHERE metadata$action = 'INSERT'
      AND metadata$isupdate = 'TRUE'
),

update_expires AS (
    SELECT 
        ch.CUSTOMER_ID,
        null AS FIRST_NAME,
        null AS LAST_NAME,
        null AS EMAIL,
        null AS STREET,
        null AS CITY,
        null AS STATE,
        null AS COUNTRY,
        ch.start_time,
        ctc.update_timestamp AS end_time,
        FALSE AS is_current,
        'U' AS dml_type
    FROM customer_history ch
    JOIN customer_table_changes ctc
    ON ch.customer_id = ctc.customer_id
    WHERE metadata$action = 'DELETE'
    AND metadata$isupdate = 'TRUE'
    AND ch.is_current = TRUE
),

deletes AS (
    SELECT 
        ch.CUSTOMER_ID,
        NULL AS FIRST_NAME,
        NULL AS LAST_NAME,
        NULL AS EMAIL,
        NULL AS STREET,
        NULL AS CITY,
        NULL AS STATE,
        NULL AS COUNTRY,
        ch.start_time,
        CURRENT_TIMESTAMP()::timestamp_ntz AS end_time,
        FALSE AS is_current,
        'D' AS dml_type
    FROM customer_history ch
    JOIN customer_table_changes ctc
      ON ch.customer_id = ctc.customer_id
    WHERE ctc.metadata$action = 'DELETE'
      AND ctc.metadata$isupdate = 'FALSE'
      AND ch.is_current = TRUE
),

all_changes as (
    SELECT * FROM new_inserts
    UNION ALL
    SELECT * FROM update_inserts
    UNION ALL
    SELECT * FROM update_expires
    UNION ALL
    SELECT * FROM deletes
)
select * from all_changes;

select * from v_customer_table_changes;

create or replace task tsk_scd_hist warehouse = COMPUTE_WH schedule = '1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
merge into customer_history ch
using v_customer_table_changes ccd -- v_customer_table_changes is a view that hold the logic that determines what to insert/update into the customer history table.
    on ch.customer_id = ccd.customer_id -- customer_id determines wherher there is unique record in the customer history table
    and ch.start_time = ccd.start_time 
when matched and ccd.dml_type = 'U' then update --indicates the record has been updated and is no longer current and the end_time needs to be stamped
    set ch.end_time = ccd.end_time,
        ch.is_current = FALSE
when matched and ccd.dml_type = 'D' then update -- Deletes are essentially logical deletes. The record is stamped and no new version is inserted.
    set ch.end_time = ccd.end_time,
        ch.is_current = FALSE
when not matched and ccd.dml_type = 'I' then insert --Inserting a new customer_id and updating an existing one both results in an insert
            (customer_id, first_name, last_name, email, street, city, state, country, start_time, end_time, is_current)
        values (ccd.customer_id, ccd.first_name, ccd.last_name, ccd.email, ccd.street, ccd.city, ccd.state, ccd.country, ccd.start_time, ccd.end_time, ccd.is_current); 

show tasks;       
alter task tsk_scd_hist suspend; --resume --suspend

-- Manipulation to check if customer history table showing values
insert into customer values (223136, 'Jessica', 'Arnold', 'tanner39@smith.com', '595 Benjamin Forge Suite 124', 'Michealstad', 'Connecticut', 'Cape Verde', current_timestamp());
update customer set first_name = 'Jessica' where customer_id =7523;
delete from customer where customer_id= 145 and first_name ='Kim';
select count(*).customer_id from customer group by customer_id having count(*) = 1;
select * from customer_history where customer_id = 223136;

select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state 
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;

select * from customer ;
select * from customer_history where is_current = FALSE;
