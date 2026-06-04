# DE ETL Pipeline

Data Engineering проект: от оперативной базы данных до дашборда в Tableau.

## Архитектура

```
MySQL (оперативная БД)
        ↓
       MRR  ← сырая копия из источника (дельта через HWM)
        ↓
       STG  ← очищенные и дедублированные данные
        ↓
       DWH  ← финальное хранилище + витрины для аналитики
        ↓
    Tableau ← дашборд
```

## Стек

| Компонент | Технология |
|-----------|-----------|
| Оперативная БД | MySQL 8.0 |
| Хранилище | PostgreSQL 15 |
| ETL оркестрация | Apache Airflow 2.8.1 |
| Контейнеризация | Docker / Docker Compose |
| BI | Tableau Public |

## Структура проекта

```
DE_etl_pipeline/
├── docker-compose.yml        # инфраструктура
├── backup.sh                 # backup для macOS/Linux/Git Bash
├── backup.ps1                # backup для Windows
├── dags/
│   ├── utils.py              # общие функции подключения и логирования
│   ├── etl_customers.py      # DAG: customers (SCD Type 2)
│   ├── etl_products.py       # DAG: products (SCD Type 1)
│   └── etl_sales.py          # DAG: sales + обновление витрин
├── sql/
│   ├── mysql/
│   │   ├── 01_init_operational.sql   # CREATE TABLE
│   │   ├── 02_customers_data.sql     # INSERT данные
│   │   ├── 03_products_data.sql      # INSERT данные
│   │   └── 04_sales_data.sql         # INSERT данные
│   └── postgres/
│       ├── 01_init_databases.sql     # CREATE DATABASE mrr, stg, dwh
│       ├── 02_init_mrr.sql           # таблицы MRR слоя
│       ├── 03_init_stg.sql           # таблицы STG слоя
│       ├── 04_init_dwh.sql           # таблицы DWH слоя + витрины
│       └── 05_init_dwh_procedures.sql # процедуры и функции
├── pgadmin/
│   └── servers.json          # автоподключение pgAdmin
└── backups/                  # папка для бэкапов
```

## Быстрый старт

### Требования
- Docker Desktop
- Docker Compose

### Запуск

```bash
# Клонировать репозиторий
git clone <repo_url>
cd DE_etl_pipeline

# Запустить все сервисы
docker-compose up -d

# Проверить статус
docker-compose ps
```

### Доступ к сервисам

| Сервис | URL | Логин | Пароль |
|--------|-----|-------|--------|
| Airflow | http://localhost:8080 | admin | admin |
| pgAdmin | http://localhost:5050 | admin@admin.com | admin |
| MySQL | localhost:3307 | etl_user | etl_pass |
| PostgreSQL | localhost:5433 | dwh_user | dwh_pass |

### Запуск ETL

В Airflow UI запустить DAG'и в порядке:
1. `etl_customers`
2. `etl_products`
3. `etl_sales`

## Слои хранилища

### MRR (Mirror)
Сырая копия данных из источника. Данные берутся через **High Water Mark** — только изменения после последней загрузки. Хранит исторические копии всех загрузок.

### STG (Staging)
Очищенный слой. Dimensions хранят актуальную версию через UPSERT. Факты добавляются инкрементально без дублей.

### DWH (Data Warehouse)
Финальный слой для аналитики:
- `dwh_dim_customers` — **SCD Type 2**, хранит историю изменений клиентов
- `dwh_dim_products` — **SCD Type 1**, перезаписывает актуальные данные
- `dwh_fact_sales` — факты продаж, только INSERT
- `dwh_mart_sales_by_country` — витрина: продажи по странам
- `dwh_mart_top_products` — витрина: топ продуктов

## ETL логика

### High Water Mark
```
Первый запуск:    HWM = 1900-01-01 -> загружаем все данные
Следующий запуск: HWM = last updated_at -> загружаем только дельту
```

### SCD Type 2 (customers)
```
Клиент переехал из Spain в France:
  Старая запись: is_current=false, valid_to=сегодня
  Новая запись:  is_current=true,  valid_from=сегодня
```

## Процедуры и функции (DWH)

```sql
-- Функция: суммарные продажи по стране
SELECT get_total_qty_by_country('Portugal');

-- Процедура: пересчёт всех витрин
CALL refresh_marts();
```

## Логирование

Каждый DAG пишет логи в таблицу `etl_logs` в каждом слое:

```sql
SELECT dag_id, task_id, status, message, finished_at
FROM etl_logs
ORDER BY finished_at DESC;
```

## Backup

### macOS / Linux / Git Bash

```bash
# Даём права на выполнение
chmod +x backup.sh

bash backup.sh
```

### Windows
```bash
.\backup.ps1
```

Бэкапы сохраняются в папку `backups/` с timestamp в имени файла:
```
backups/
├── mrr_2026-06-05_00-52-11.sql
├── stg_2026-06-05_00-52-11.sql
└── dwh_2026-06-05_00-52-11.sql
```

## Tableau Dashboard

[Ссылка на дашборд](<https://public.tableau.com/app/profile/alexandr.belov/viz/SalesAnalyticstest/SalesAnalytics?publish=yes>)

```Tableau Public не поддерживает прямое подключение к локальному PostgreSQL — данные экспортированы из витрин DWH в CSV. В production окружении с Tableau Desktop подключение было бы напрямую к dwh через порт 5433.```

Два листа:
- **Sales by Country** — топ стран по объёму продаж
- **Top 10 Products** — топ 10 продуктов по категориям
