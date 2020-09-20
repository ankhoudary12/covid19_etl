"""Repository of queries used in DAGS."""
# Drop Queries

drop_country = "DROP TABLE IF EXISTS covid.dim_country CASCADE"
drop_state = "DROP TABLE IF EXISTS covid.dim_state CASCADE"
drop_city = "DROP TABLE IF EXISTS covid.dim_city CASCADE"
drop_coordinates = "DROP TABLE IF EXISTS covid.dim_coordinates CASCADE"
drop_covid = "DROP TABLE IF EXISTS covid.fact_covid CASCADE"
drop_staging_covid = "DROP TABLE IF EXISTS covid.staging_raw_covid CASCADE"

# Create Table Queries

create_country = """

CREATE TABLE IF NOT EXISTS covid.dim_country (
    country_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    country VARCHAR
)

"""

create_state = """

CREATE TABLE IF NOT EXISTS covid.dim_state (
    state_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    state VARCHAR
)

"""

create_city = """

CREATE TABLE IF NOT EXISTS covid.dim_city (
    city_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR
)

"""

create_coordinates = """

CREATE TABLE IF NOT EXISTS covid.dim_coordinates (
    coordinates_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
)

"""

create_covid = """

CREATE TABLE IF NOT EXISTS covid.fact_covid (
    date DATE,
    country_id INTEGER,
    state_id INTEGER,
    city_id INTEGER,
    coordinates_id INTEGER,
    confirmed INTEGER,
    deaths INTEGER,
    recovered INTEGER,
    active INTEGER,
    incidence_rate NUMERIC
)

"""

create_staging_covid = """

CREATE TABLE IF NOT EXISTS covid.staging_raw_covid (
    fips INTEGER,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    last_update DATE,
    latitude NUMERIC,
    longitude NUMERIC,
    confirmed INTEGER,
    deaths INTEGER,
    recovered INTEGER,
    active INTEGER,
    location VARCHAR,
    incidence_rate NUMERIC,
    case_fatality_ratio NUMERIC
)

"""

# Insert Queries

insert_country = """

INSERT INTO covid.dim_country (country)

(SELECT
    DISTINCT country
FROM covid.staging_raw_covid
WHERE country NOT IN (SELECT country FROM covid.dim_country))



"""

insert_state = """

INSERT INTO covid.dim_state (state)

(SELECT
    DISTINCT state
FROM covid.staging_raw_covid
WHERE state NOT IN (SELECT state FROM covid.dim_state))



"""

insert_city = """

INSERT INTO covid.dim_city (city)

(SELECT
    DISTINCT city
FROM covid.staging_raw_covid
WHERE city NOT IN (SELECT city FROM covid.dim_city))



"""

insert_coordinates = """

INSERT INTO covid.dim_coordinates (location, latitude, longitude)

(SELECT
    DISTINCT location,
    latitude,
    longitude
FROM covid.staging_raw_covid
WHERE location NOT IN (SELECT location FROM covid.dim_coordinates))


"""

insert_covid = """

INSERT INTO covid.fact_covid (date, city_id, state_id, country_id, \
coordinates_id, confirmed, deaths, recovered, active, incidence_rate)

SELECT
    DATE(s.last_update) AS date,
    ci.city_id,
    st.state_id,
    c.country_id,
    coor.coordinates_id,
    s.confirmed,
    s.deaths,
    s.recovered,
    s.active,
    s.incidence_rate
FROM covid.staging_raw_covid s
LEFT JOIN covid.dim_country c ON c.country=s.country
LEFT JOIN covid.dim_state st ON st.state=s.state
LEFT JOIN covid.dim_city ci ON ci.city=s.city
LEFT JOIN covid.dim_coordinates coor ON coor.location=s.location

"""


insert_staging_covid = """
COPY covid.staging_raw_covid
FROM '{}'
REGION 'us-west-2'
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
DELIMITER '|'
EMPTYASNULL
NULL AS '\'\''
TIMEFORMAT 'auto'
REMOVEQUOTES
IGNOREHEADER 1
"""

# Re-Updating fact_covid query

delete_fact_covid = """
DELETE FROM covid.fact_covid
WHERE date BETWEEN (SELECT MIN(last_update) FROM covid.staging_raw_covid)
AND
(SELECT MAX(last_update) FROM covid.staging_raw_covid)
"""

# Validation Queries

count_country = "SELECT COUNT(*) FROM covid.dim_country"
count_state = "SELECT COUNT(*) FROM covid.dim_state"
count_city = "SELECT COUNT(*) FROM covid.dim_city"
count_coordinates = "SELECT COUNT(*) FROM covid.dim_coordinates"
count_covid = "SELECT COUNT(*) FROM covid.fact_covid"

# Create lists of the queries for easy access later
drop_queries = [
    drop_country,
    drop_state,
    drop_city,
    drop_coordinates,
    drop_covid,
    drop_staging_covid,
]

create_queries = [
    create_country,
    create_state,
    create_city,
    create_coordinates,
    create_covid,
    create_staging_covid,
]

insert_queries = [
    insert_country,
    insert_state,
    insert_city,
    insert_coordinates,
    insert_covid,
]

validate_queries = [
    count_country,
    count_state,
    count_city,
    count_coordinates,
    count_covid,
]
