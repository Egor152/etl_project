# Проект 3-го спринта

### Структура репозитория
1. Папка `migrations` хранит файлы миграции. Файлы миграции должны быть с расширением `.sql` и содержать SQL-скрипт обновления базы данных.
2. В папке `src` хранятся все необходимые исходники: 
    * Папка `dags` содержит DAG's Airflow.

### Как запустить контейнер
Запустите локально команду:

```
docker run -d --rm -p 3000:3000 -p 15432:5432 скрыл по просьбе Яндекс Практикума
```

После того как запустится контейнер, у вас будут доступны:
1. Visual Studio Code
2. Airflow
3. Database
