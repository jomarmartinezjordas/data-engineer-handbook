-- PostgreSQL sink table
CREATE TABLE sessionized_events (
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    num_events BIGINT 
);

-- DROP TABLE IF EXISTS sessionized_events;

--Check the data
SELECT * FROM sessionized_events;

-- Q1: What is the average number of web events of a session from a user on Tech Creator?
SELECT
    AVG(num_events) AS avg_events_per_session
FROM sessionized_events
WHERE host LIKE '%techcreator.io';

-- Q2: Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
SELECT
    host,
    AVG(num_events) AS avg_events_per_session
FROM sessionized_events
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC;
