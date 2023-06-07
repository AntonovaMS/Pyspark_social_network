# Pyspark_social_network
Витрины для соцсети в разрезе активности пользователей по городам , а также витрина рекомендаций друзей для пользователей.
Airflow DAG для автоматизации обновления витрин.
Из HDFS берем таблицу событий (parquet) и таблицу координат городов Австралии (csv)
- zadanie2: Витрина в разрезе пользователей и их локаций:
   - актуальный адрес события
   - домашний адрес
   - количество посещённых городов
   - список городов в порядке посещения
- zadanie3: Витрина в разрезе зон
   - количество сообщений, количество подписок, количество регистраций в разрезе зон и периода(месяц, неделя)
- zadanie4: Витрина для рекомендации друзей
   - если пользователи подписаны на один канал, ранее никогда не переписывались и расстояние между ними не превышает 1 км
Записываем витрины в формате parquet в папку для аналитиков
