CREATE OR REPLACE FUNCTION load_channels()
RETURNS void AS $$
BEGIN
	
    -- Создаем временную таблицу new_channels, чтобы хранить новые каналы	
    CREATE TEMP TABLE new_channels AS
    SELECT s.channel_id, s.channel_title, s.channel_description
    FROM staging.channels s
    WHERE s.channel_id NOT IN (SELECT dc.channel_id FROM data_mart.dim_channel dc);

    -- Создаем временную таблицу existing_channels, чтобы хранить существующие каналы
    CREATE TEMP TABLE existing_channels AS
    SELECT s.channel_id
    FROM staging.channels s
    WHERE s.channel_id IN (SELECT dc.channel_id FROM data_mart.dim_channel dc);
   
    UPDATE data_mart.dim_channel AS d
    SET
        channel_title = s.channel_title,
        channel_description = s.channel_description
    FROM staging.channels AS s
    WHERE d.channel_id = s.channel_id
    AND s.channel_id IN (SELECT channel_id FROM existing_channels);
    
    INSERT INTO data_mart.dim_channel AS d (channel_id, channel_title, channel_description)
    SELECT s.channel_id, s.channel_title, s.channel_description
    FROM staging.channels AS s
    WHERE s.channel_id IN (SELECT channel_id FROM new_channels);
   
    DROP TABLE new_channels;
    DROP TABLE existing_channels;
   
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION load_dates()
RETURNS void AS $$
BEGIN

    INSERT INTO data_mart.dim_date ("date", "year", "month", "day")
    SELECT
        date_trunc('day', published_at) AS "date",
        EXTRACT(YEAR FROM published_at) AS "year",
        EXTRACT(MONTH FROM published_at) AS "month",
        EXTRACT(DAY FROM published_at) AS "day"
    FROM
        staging.channels 
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_date
        WHERE "date" = date_trunc('day', published_at)
    );
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION load_lang()
RETURNS void AS $$
BEGIN          
    INSERT INTO data_mart.dim_language (default_audio_language)
    SELECT DISTINCT v.default_audio_language
    FROM staging.videos AS v
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_language AS d
        WHERE d.default_audio_language = v.default_audio_language OR (d.default_audio_language IS NULL AND v.default_audio_language IS NULL)
    );
END
$$ LANGUAGE plpgsql;

SELECT pg_get_serial_sequence('dim_language', 'lang_id');
SELECT setval('dim_language_lang_id_seq', 1, false);

CREATE OR REPLACE FUNCTION load_query()
RETURNS void AS $$
BEGIN          
    INSERT INTO data_mart.dim_query (query_text)
    SELECT DISTINCT q.query
    FROM staging.queries AS q
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_query AS d
        WHERE d.query_text  = q.query
    );
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION load_videos()
RETURNS void AS $$
BEGIN
	CREATE TEMP TABLE lang_ids_check AS (
        SELECT sv.video_id, sv.video_title, sv.video_description, sv.video_duration, sv.tags, dl.lang_id
        FROM staging.videos sv 
        LEFT JOIN data_mart.dim_language dl
        ON sv.default_audio_language IS NOT DISTINCT FROM dl.default_audio_language
    );
   
    INSERT INTO data_mart.dim_video (video_id, video_title, video_description, video_duration, lang_id, tags)
    SELECT lic.video_id, lic.video_title, lic.video_description, lic.video_duration, lic.lang_id, lic.tags
    FROM lang_ids_check AS lic
    WHERE NOT EXISTS (
        SELECT 1
        FROM data_mart.dim_video dv
        WHERE dv.video_id = lic.video_id
    );
   
    DROP TABLE lang_ids_check;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION load_video_query()
RETURNS void AS $$
BEGIN
    CREATE TEMP TABLE queries_check AS (
    SELECT q.video_id, dq.query_id
    FROM staging.queries q 
    JOIN data_mart.dim_query dq 
    ON q.query = dq.query_text
    );
    
    INSERT INTO dim_video_query (video_id, query_id)
    SELECT video_id, query_id 
    FROM queries_check qc
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_video_query dq
        WHERE dq.video_id = qc.video_id AND dq.query_id = qc.query_id
    );

    DROP TABLE queries_check;

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION load_video_stats()
RETURNS void AS $$
BEGIN 
	INSERT INTO fact_video_stat (video_id, channel_id, date_id, viewcount, likecount, commentcount)
    SELECT sv.video_id, sv.channel_id, dd.date_id, sv.viewcount, sv.likecount, sv.commentcount
    FROM staging.videos sv
    JOIN dim_date dd 
    ON dd."date" = date_trunc('day', published_at)
    WHERE NOT EXISTS (
        SELECT 1
        FROM fact_video_stat fcs
        WHERE fcs.video_id = sv.video_id
    );
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION load_channel_stats()
RETURNS void AS $$
BEGIN 
	
	CREATE TEMP TABLE fact_channel_check AS (
	    SELECT sc.channel_id, dd.date_id, sc.video_count, sc.subscriber_count
        FROM staging.channels sc
        LEFT JOIN dim_date dd 
        ON dd."date" = date_trunc('day', published_at)
        WHERE NOT EXISTS (
            SELECT 1
            FROM fact_channel_stat fcs
            WHERE fcs.channel_id = sc.channel_id
        )
    );

	DELETE FROM data_mart.fact_channel_stat AS fcs
	WHERE EXISTS (
	  SELECT 1
	  FROM fact_channel_check fcc
	  WHERE fcs.channel_id = fcc.channel_id
	);
	
	INSERT INTO data_mart.fact_channel_stat (channel_id, date_id, subscriber_count, video_count)
    SELECT fcc.channel_id, fcc.date_id, fcc.subscriber_count, fcc.video_count
    FROM fact_channel_check fcc;

    DROP TABLE fact_channel_check;

END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION load_dates(schema_name text, table_name text)
RETURNS void AS $$
BEGIN
    EXECUTE format('
        INSERT INTO data_mart.dim_date ("date", "year", "month", "day")
        SELECT
            date_trunc(''day'', published_at) AS "date",
            EXTRACT(YEAR FROM published_at) AS "year",
            EXTRACT(MONTH FROM published_at) AS "month",
            EXTRACT(DAY FROM published_at) AS "day"
        FROM
            %I.%I
        WHERE NOT EXISTS (
            SELECT 1
            FROM dim_date
            WHERE "date" = date_trunc(''day'', published_at)
        );', schema_name, table_name, schema_name, table_name);
END
$$ LANGUAGE plpgsql;

SELECT load_dates('staging', 'channels');

