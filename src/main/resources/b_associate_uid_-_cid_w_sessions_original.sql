/*
create table `tui-segmentstream.end2end_data_pipeline.b_associate_uid_-_cid_w_sessions_original` as

*/


-- TRUNCATE-=> APPEND
-- Выдергиваем пересечения параметров  проводим ассоцицию с session_id

-- Сначала большая загрузка данных все до СЕГОДНЯ ( -3 TRUNCATE
-- Далее загрузка по CRON - СЕГОДНЯ( -2 APPEND
-- Таблица уникальных
-- UID  CID  Session_ID ассоциации из событий кликстрима и офлайн событий (uid  timestamp . Окно ретроспективы.

-- ОКНО РЕТРОСПЕКТИВЫ - 30 дней


-- create or replace table `tui-segmentstream.end2end_data_pipeline.b_associate_uid_-_cid_w_sessions_original` as

WITH
 query_params AS (
    -- Параметры запроса определяем здесь  чтобы сто раз не писать их в тексте
    SELECT
        -- Начало отчетного диапазона. В GDS поставляем как PARSE_DATE('%Y%m%d'  @DS_START_DATE
        CURRENT_DATE( -100 AS start_date
        -- Конец отчетного диапазона. В GDS поставляем как PARSE_DATE('%Y%m%d'  @DS_END_DATE
        CURRENT_DATE( -3 AS end_date
        -- Начало окна ретроспективы. В GDS поставляем как DATE_SUB(PARSE_DATE('%Y%m%d'  @DS_END_DATE   INTERVAL 90 DAY
        DATE_SUB(CURRENT_DATE(   INTERVAL 100 DAY  AS retro_date


    -- задача вытащить puid из разных ивентов и создать нужные ассоциации для uid  cid  session_id где это пропущено
raw_data_norm AS (

    -- ***************
    -- FLOCKTORY EVENTS + обычные события сайта + usedesk
    -- ***************
    -- Выдергиваем PUID  cid  session_id из кликстрима  там где есть ивент флоктори и события форм сайта
    SELECT
    DISTINCT
    CONCAT
    (user_pseudo_id  '.' (SELECT value.int_value FROM UNNEST(event_params  WHERE key = 'ga_session_id'   AS session_id
    user_pseudo_id AS client_id
    (SELECT value.string_value FROM UNNEST(event_params  WHERE key = 'puid'  AS p_uid
    TIMESTAMP_MICROS(event_timestamp  as event_timestamp
    start_date
    retro_date
    end_date
    FROM `tui-segmentstream.analytics_305654698.events_*` 
      CROSS JOIN query_params
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d'  retro_date  AND FORMAT_DATE('%Y%m%d'  end_date
    AND user_pseudo_id LIKE '%.%'
    AND event_name IN ('page_view' 'session_start'  'first_visit'  'flocktory_collected_id'
    'tour_select_help' 'fast_reg' 'book_reg' 'auth'


-- ***************
-- CALLTOUCH добавляем события колбеков для наполнения полноты UID упоминаний (для кейсов где нет session_id
-- ***************


    UNION ALL
    SELECT
    null as session_id
    client_id
    p_uid
    TIMESTAMP_MICROS(event_ts  as event_timestamp
    start_date
    retro_date
    end_date
    FROM
    (   SELECT
        event_ts  -- ts в секундах
        'calltouch_callback' as event_name
        ga_client_id as client_id
        p_uid
        TIMESTAMP_MICROS(event_ts  as date
        FROM `tui-segmentstream.ad_costs.calltouch_callback_uids_cid`
      f
    CROSS JOIN query_params
    WHERE f.event_name = 'calltouch_callback'


-- ***************
-- USEDESK добавляем события чатов для наполнения полноты UID упоминаний (для кейсов где нет session_id
-- ***************


    UNION ALL

    SELECT
    null as session_id
    client_id
    p_uid
    TIMESTAMP_MICROS(event_ts  as event_timestamp
    start_date
    retro_date
    end_date
    FROM
    (   SELECT
        event_ts
        'usedesk_messenger' as event_name
        client_id
        p_uid
        TIMESTAMP_MICROS(event_ts  as date
        FROM
            (
                SELECT
                UNIX_MICROS(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S'  created_at   as event_ts
                created_at
                client_id
                p_uid
                FROM
                (
                    SELECT 
                    DISTINCT 
                    CAST(created_at AS STRING  as created_at
                    email as e_uid
                    ga_id as client_id
                    mobile_phone as p_uid
                    ROW_NUMBER(  OVER(PARTITION BY email ORDER BY created_at  as rn
                    FROM 
                    `tui-segmentstream.ad_costs.usedesk_conn`
                    WHERE ga_id is not null
                  t
                WHERE rn = 1

      f
    CROSS JOIN query_params


    

-- Создаем таблицу просто ассоциаций uid  cid  session_id
assosiate_uid_to_cid AS (
    SELECT 
    DISTINCT
    assosiated_session_id
    MAX(assosiated_p_uid  as assosiated_p_uid
    client_id
     FROM (
    -- Ассоциируем UID на CID пользователя
    SELECT 
        -- Все из предыдущей таблицы
        raw_data_norm.*
        -- Берем последний указанный UID для каждого CID используя TS
        LAST_VALUE(p_uid IGNORE NULLS  OVER (PARTITION BY client_id ORDER BY event_timestamp ASC  AS assosiated_p_uid
        -- в определенных строках нет session_id   его мы также вытаскиваем из ближайших упоминаний
        LAST_VALUE(session_id IGNORE NULLS  OVER (PARTITION BY client_id ORDER BY event_timestamp ASC  AS assosiated_session_id
        FROM raw_data_norm
        WHERE
    -- Фильтранём по требуемым датам
    EXTRACT(DATE FROM event_timestamp  BETWEEN start_date AND end_date
    -- Для отладки  построим только ассоциированные UID
    AND assosiated_session_id IS NOT NULL
    AND assosiated_p_uid IS NOT NULL
    GROUP BY 
    assosiated_session_id
    client_id
 



SELECT *
FROM assosiate_uid_to_cid