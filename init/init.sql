-- CORREGIDO: crear estaciones primero, luego logs que referencian estaciones.
CREATE TABLE IF NOT EXISTS weather_stations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    city VARCHAR(150),
    country VARCHAR(150),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS weather_logs (
    id SERIAL PRIMARY KEY,
    id_station INTEGER NOT NULL REFERENCES weather_stations(id),
    dates TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature_celsius REAL,
    humidity REAL,
    wind VARCHAR(5),
    wind_speed REAL,
    pressure REAL
);
INSERT INTO weather_stations (name, city, country, latitude, longitude, altitude) VALUES
('Estacion Norte', 'Ciudad A', 'Colombia', 4.700, -74.050, 2550),
('Estacion Sur',  'Ciudad B', 'Chile', 4.500, -74.100, 2450),
('Estacion Este', 'Ciudad C', 'Argentina', 4.650, -73.950, 2600),
('Estacion Oeste', 'Ciudad D', 'Romania', 4.720, -74.200, 2400),
('Estacion Central','Ciudad E', 'Colombia', 4.680, -74.080, 2500);