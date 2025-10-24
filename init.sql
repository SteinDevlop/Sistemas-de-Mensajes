CREATE TABLE IF NOT EXISTS weather_logs (
    id INT PRIMARY KEY,
    dates datetime,
    temperature_celsius FLOAT,
    humidity FLOAT,
    wind VARCHAR(5),
    wind_speed FLOAT,
    pressure FLOAT
);