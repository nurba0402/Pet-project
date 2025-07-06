import requests
import psycopg2
from datetime import datetime, timezone


api_key = "01013336610a7454cb49f139a6d77a2d"
cities = ["Bishkek", "Osh", "Almaty", "Tashkent", "Moscow", "London", "New York", "Tokyo", "Paris", "Dubai"]


db_config = {
    "host": "host.docker.internal",
    "port": "5437",
    "database": "postgres",
    "user": "postgres",
    "password": "123"
}

def get_weather_data(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    r = requests.get(url)
    if r.status_code == 200:
        data = r.json()
        return {
            "city": city,
            "datetime": datetime.fromtimestamp(data["dt"], tz=timezone.utc),
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"]
        }
    else:
        print(f"Error for {city}: {r.status_code}")
        return None

def save_to_db(records):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    for rec in records:
        cur.execute("""
            INSERT INTO weather.weather_raw (city, dt_utc, temperature, humidity, wind_speed)
            VALUES (%s, %s, %s, %s, %s);
        """, (rec["city"], rec["datetime"], rec["temperature"], rec["humidity"], rec["wind_speed"]))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    all_data = []
    for city in cities:
        data = get_weather_data(city)
        if data:
            all_data.append(data)
    if all_data:
        save_to_db(all_data)