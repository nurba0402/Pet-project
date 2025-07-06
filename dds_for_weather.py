import psycopg2
from datetime import datetime, timezone

db_config = {
    "host": "host.docker.internal",
    "port": "5437",
    "database": "postgres",
    "user": "postgres",
    "password": "123"
}

def load_weather_data():
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
        insert into dds.dim_city (city_name)
        select distinct wr.city
        from weather.weather_raw wr
        left join dds.dim_city dc on wr.city = dc.city_name
        where dc.city_name is null;
    """)
    cursor.execute("""
        insert into dds.fact_weather (city_id, dt_utc, temperature, humidity, wind_speed)
        select dc.city_id, wr.dt_utc, wr.temperature, wr.humidity, wr.wind_speed
        from weather.weather_raw wr
        join dds.dim_city dc on wr.city = dc.city_name
        on conflict (city_id, dt_utc) do nothing;
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print(f'Weather data loaded successfully.')

if __name__ == "__main__":
    load_weather_data()
    print(f'Weather data loading completed at {datetime.now(timezone.utc)}')