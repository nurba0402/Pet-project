
# Пет проект Дата инженера.
В этом проекте я создаю свой первый проект в качестве дата инженера.
В основе лежат реальные данные о погоде из сайта OpenWeather

Используемые интрументы: Dbeaver Postgresql, VS code Python, Apache Airflow, Docker, Power BI.

## Этапы проекта:

- Архитектрура проекта
- Регистрация на сайте и получаем API
- Создание схемы и таблицы в Postgresql
- Пишем код ETL- процессов c Python 
- Создание dag для оркестрации ETL в Airflow 
- Построение дашбордов для аналитики в Power BI

## Архитектура проекта
![Схема Архитектуры проекта](https://github.com/user-attachments/assets/c4263006-26e2-459a-b715-351c2bc0a2e9)

## Создание схемы и таблицы в Postgresql

```sql
create schema if not exists weather;
create schema if not exists dds;

create table weather.weather_raw (
    weather_id serial primary key,
    city varchar,
    dt_utc timestamp,
    temperature float4,
    humidity int,
    wind_speed float4,
    created_at timestamp default now()
);


create table dds.dim_city (
    city_id serial primary key,
    city_name varchar(30) unique
);

create table dds.fact_weather (
    fact_id serial primary key,
    city_id int references dds.dim_city(city_id),
    dt_utc timestamp,
    temperature float4,
    humidity int,
    wind_speed float4,
    unique (city_id, dt_utc)
```

## Пострение ETL
Пишем скрипт `weather_collector.py` - получение данных из API и загрузки в ODS
библиотеки для:
* получения url-сайта через `requests`
* подключение к БД через psycopg2
* datetime, timezone для получения данных о времени и о часовом поясе
   
```python
import requests
import psycopg2
from datetime import datetime, timezone
```
Получились две функции из которых:
1. Берет данные о городе, даты-временени и часового пояса, температуры, влажности, скорости ветра
2. Полученные записи сохраняются на прежде созданной схеме ODS в таблице `weather_raw` в postgres

Пишем скрипт `dds_for_weather.py` - загрузка данных в слой DDS из ODS
библиотеки:
* psycopg2
* datetime, timezone
```python
import psycopg2
from datetime import datetime, timezone
```
Создал функцию с SQL-логикой внутри:
* Данные загружаются на нормализованную схему DDS (таблица фактов с одной зависимостью.)

К схеме `dds` добавляем таблицу логов Data quality которое создается при загрузки данных `weather_data_quality.py`
библиотеки:
* psycopg2
* datetime, timezone
```python
import psycopg2
from datetime import datetime
```
Написана одна функция с двумя логиками - проверка на NULL и проверка на дубликаты.

## Пострение дашбордов для аналитики в Power BI
`Пет проект.pbix`
* Добавлено вычисляемое поле `HourOnly` - выводит только часы(HH:00).
* Создан фильтр по городу. Созданы меры для изменения название города по фильтру в заголовке
### Дашборды
- Средняя температура по часам
- Влажность в процентах по часам
- Скорость ветра в мс по часам
- Геометка

  


