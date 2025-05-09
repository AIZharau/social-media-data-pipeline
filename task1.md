Задание 1: Интеграция с внешним API TikTok с учетом масштабируемости
Необходимо разработать ETL-конвейер для сбора данных
о пользователях из TikTok. Данные должны выгружаться каждый час с использованием API.
Задачи:
1. Реализовать интеграцию с API TikTok:
* Собрать данные о лайках, просмотрах, репостах для списка пользователей.
* Оптимизировать работу с API (использование параллельных запросов, кеширование токенов аутентификации).
* Обрабатывать ограничения API (rate limits, ошибки 429, 503).
2. Спроектировать хранилище данных:
* Cтруктурировать таблицы в PostgreSQL?
* Как избежать дублирования данных?
* Как спроектировать партиционирование и индексацию?
3. Реализовать обработку в реальном времени:
* Какие инструменты потоковой обработки (Kafka, Spark Streaming, Flink) и как использовать?
* Как реализовать мониторинг ETL? (например, Prometheus + Grafana)
4. Оценить производительность:
* Как обрабатывать большой объем записей в сутки?
* Как минимизировать нагрузку на БД?
Ссылки на аккаунты: 
Саша Обществознайка
https://www.tiktok.com/@obschestvoznaika_el?_t=ZS-8s811wf5v9R&_r=1
Женя Химичка
https://www.tiktok.com/@himichka_el?_t=8s8CWkOM7Fw&_r=1
Кико Англичанка
https://www.tiktok.com/@anglichanka_el?_t=8s8CQXHFybf&_r=1
Макс Физик
https://www.tiktok.com/@fizik_el?_t=ZS-8s85SvseEQE&_r=1
Катя Математичка
https://www.tiktok.com/@katya_matematichka?_t=ZS-8s85aIz7A4F&_r=1

[ОТВЕТ](./task1/README.md)