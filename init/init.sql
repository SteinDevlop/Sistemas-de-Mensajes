
CREATE TABLE IF NOT EXISTS weather_logs (
    id INT PRIMARY KEY,
    id_station INT FORAIGN KEY weather_stations(id)
    dates datetime,
    temperature_celsius FLOAT,
    humidity FLOAT,
    wind VARCHAR(5),
    wind_speed FLOAT,
    pressure FLOAT
);

CREATE TABLE IF NOT EXISTS weather_stations (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    city VARCHAR(150),
    contry VARCHAR(150)M
    latitude FLOAT,
    longitude FLOAT,
    altitude FLOAT,
);
