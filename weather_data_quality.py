from datetime import datetime
import psycopg2

db_config = {
    "host": "host.docker.internal",
    "port": "5437",
    "database": "postgres",
    "user": "postgres",
    "password": "123"
}

def quality_check():
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    result = {'null_check': True, 'duplicate_check': True}
    log = []

    cur.execute(
        """select count(*)
           from dds.fact_weather
           where city_id is null or dt_utc is null
            or temperature is null or humidity is null or wind_speed is null""",
    )
    
    null_count = cur.fetchone()[0]
    if null_count > 0:
        result['null_check'] = False
        log.append(f"Проверка на Null: {null_count} найдено.")
    else:
        log.append("Проверка на Null: все поля заполнены.")

    cur.execute(
        """select city_id, dt_utc, count(*)
        from dds.fact_weather
        group by city_id, dt_utc
        having count(*) > 1"""
    )

    duplicate_count = cur.fetchall()
    if duplicate_count:
        result['duplicate_check'] = False
        log.append(f"Проверка на дубликаты: {len(duplicate_count)} найдено.")
    else:
        log.append("Проверка на дубликаты: дубликатов не найдено.")


    cur.execute(
        """create table if not exists dds.quality_checks (
            check_id serial primary key,
            check_time timestamp default current_timestamp,
            null_check boolean,
            duplicate_check boolean
        )"""
    )

    cur.execute(
        """insert into dds.quality_checks (check_time, duplicate_check, null_check)
           values (current_timestamp, %s, %s)""",
        (result['duplicate_check'], result['null_check'])
    )

    conn.commit()
    cur.close()
    conn.close()
    print("Проведена проверка качества данных")

if __name__ == "__main__":
    quality_check()