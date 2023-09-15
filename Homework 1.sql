SELECT * FROM actor_films

DROP TABLE actors

CREATE TYPE films AS (
                         film TEXT,
                         votes INTEGER,
                         rating REAL,
                         filmid TEXT
                         )
                         
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad')

CREATE TABLE actors (
	 actor TEXT,
	 actorid TEXT,
	 films films[],
	 quality_class quality_class,
	 is_active BOOLEAN,
	 year INTEGER,
PRIMARY KEY(actorid, year)
)


WITH last_year AS(
SELECT * 
FROM actors
WHERE year = 2020),

this_year AS(
SELECT actor, actorid, array_agg(row(film, votes, rating, filmid)::films) AS films, year 
FROM actor_films
WHERE year = 2021
GROUP BY actor, actorid, year
),

coalesced AS(
SELECT  
	COALESCE(ty.actor, ly.actor) AS actor, 
	COALESCE(ty.actorid, ly.actorid) AS actorid,
	CASE WHEN ty.films IS NULL THEN ly.films
		WHEN ly.films IS NULL THEN ty.films
		ELSE ly.films || ty.films END AS films,
	CASE WHEN ty.actorid IS NOT NULL THEN TRUE 
		 ELSE FALSE END AS is_active,
	2021 AS this_year
	FROM last_year ly
	FULL OUTER JOIN  this_year ty ON
	ly.actorid = ty.actorid),

all_ratings AS (
SELECT actor, actorid, 
films, (UNNEST(films[:])::films).rating AS rating,
is_active,
this_year
FROM coalesced
)

INSERT INTO actors
SELECT actor,
actorid,
films,
CASE WHEN avg(rating) > 8 THEN 'star'
                    WHEN avg(rating) > 7 THEN 'good'
                    WHEN avg(rating) > 6 THEN 'average'
                    ELSE 'bad' END::quality_class quality_class,
is_active,
this_year
FROM all_ratings
GROUP BY 
actor, 
actorid,
films,
is_active,
this_year

SELECT * FROM actors



DROP TABLE actors_history_scd

CREATE TABLE actors_history_scd(
actor TEXT,
actorid TEXT,
quality_class quality_class,
is_active BOOLEAN,
start_date INTEGER,
end_date INTEGER,
year INTEGER,
PRIMARY KEY(actorid, start_date)
)

select * from actors_history_scd

CREATE TYPE scd_type AS (

			quality_class quality_class,
			is_active boolean,
			start_date INTEGER,
			end_date INTEGER
			)
			

SELECT * FROM actors
			
WITH with_previous AS(
SELECT actor, actorid, quality_class,  
LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY year) as previous_quality_class,
is_active, 
LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY year) as previous_is_active,
year
FROM actors
WHERE year <= 2021
),


with_indicators AS (
SELECT *, 
	CASE WHEN quality_class <> quality_class THEN 1
		WHEN is_active <> previous_is_active THEN 1
		ELSE 0
	END AS change_indicator
FROM with_previous 
),

with_streaks AS (SELECT *, SUM(change_indicator) OVER (PARTITION BY actor ORDER BY year) AS streak_identifier
FROM with_indicators
)

INSERT INTO actors_history_scd
SELECT actor, actorid, quality_class, is_active,  
MIN(year) as start_date, 
MAX(year) as end_date,
2021 AS year
FROM with_streaks
GROUP BY actor, actorid, streak_identifier, is_active, quality_class
ORDER BY actor, streak_identifier


select * from actors_history_scd



