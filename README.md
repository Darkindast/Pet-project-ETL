# Стек технологий

- **Airflow** – оркестрация ETL  
- **Docker / Docker Compose** – контейнеризация  
- **Python** – обработка данных  
- **Pandas** – фильтрация и агрегация CSV  
- **PostgreSQL** – хранение результатов  
- **Kaggle** – данные с Kaggle  

---

## 1️⃣ Создание `docker-compose.yaml`

Закидываем созданный файл `docker-compose.yaml` в рабочий каталог.

---

## 2️⃣ Запуск контейнеров

Открываем WSL (так как вы на Windows), где установлен Docker, переходим в каталог с `docker-compose.yaml` и выполняем команду:

```bash
docker compose up -d
```

---

## 3️⃣ Подготовка DAG

- Пока контейнеры поднимаются, создаем файл `dag.py` (наш DAG).
- Для аналитики будем пользоваться файликом с kaggle `global_housing.csv`:
<img width="766" height="545" alt="image" src="https://github.com/user-attachments/assets/571b02df-5b4e-49c3-8ea7-f429b77aa7fb" />

- Разделим наш DAG на **3 основные задачи**, так как файлик с kaggle мы уже загрузили:
  1. **Создание таблицы в PostgreSQL**  
     - Используется `PostgresOperator`.  
     - Таблица `analytics_table` создается с необходимыми колонками для аналитики.  
     - Если таблица уже существует, она пересоздается.

  2. **Подготовка аналитической таблицы**  
     - Используется `PythonOperator`.  
     - Загружается CSV `global_housing.csv` с помощью `pandas`.  
     - Создаются новые колонки: `Price_to_Rent_Ratio` и `Adjusted_Affordability`.  
     - Фильтруются данные (например, только США после 2015 года).  
     - Временный CSV сохраняется в `/tmp/analytics_table.csv` и путь передается через XCom.

  3. **Загрузка аналитической таблицы в PostgreSQL**  
     - Используется `PythonOperator`.  
     - Читается временный CSV с аналитическими данными.  
     - Данные загружаются в таблицу `analytics_table` с помощью `SQLAlchemy`.  
  
- После того как контейнеры запустятся, в каталоге с `docker-compose.yaml` появятся четыре каталога:  

  - `config`  
  - `dags`  
  - `logs`  
  - `plugins`  

- Переместите файл `dag.py` в каталог `dags`.

---

## 4️⃣ Доступ к Airflow

Откройте браузер и в адресной строке введите:

http://localhost:8080


Используйте следующие учетные данные для входа:

- **Логин:** airflow  
- **Пароль:** airflow  

Вы попадёте на стартовую страницу Airflow, где будет доступен ваш DAG из файла `dag.py`.

---

## 5️⃣ Настройка подключения к PostgreSQL

Перед тем как запускать DAG, необходимо создать подключение к базе данных PostgreSQL, которая присутствует в `docker-compose.yaml`.

1. В веб-интерфейсе Airflow нужно перейти в **Admin** и выберать **Connections**.  
2. Нажми синий **+** для добавления нового подключения и заполните поля следующим образом:

| Параметр        | Значение                       |
|-----------------|--------------------------------|
| **Connection Id** | psql_connection               |
| **Connection Type** | Postgres                     |
| **Host**         | host.docker.internal           |
| **Database**     | user                           |
| **Login**        | user                           |
| **Password**     | user                           |
| **Port**         | 5431                           |
| **Extra**        |               |

После сохранения подключения DAG сможет использовать эту базу для загрузки данных.

---

## 6️⃣ Запуск DAG

- После настройки подключения к базе данных можно запустить DAG:

  1. На странице Airflow найдите ваш DAG (`dag.py`).  
  2. Нажмите на **Trigger DAG** (треугольник) в правом верхнем углу страницы.  

-DAG начнёт выполняться, и вы сможете отслеживать прогресс и логи задач в интерфейсе:

<img width="974" height="169" alt="image" src="https://github.com/user-attachments/assets/d3c2ca9d-333b-4899-92f7-f176a6fc8b56" />
<img width="974" height="397" alt="image" src="https://github.com/user-attachments/assets/203aeea6-edd6-4624-b523-6cf6e602ae06" />

-Посмотрим, какая табличка у нас получилась:
<img width="974" height="252" alt="image" src="https://github.com/user-attachments/assets/1f89d3ac-ee08-4a86-b8db-bc94e3bbe597" />





