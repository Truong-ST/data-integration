# data-integration
tourist information portal


# Run as standalone
```commandline
# Init DB
airflow db init

# create admin user
airflow users  create --role Admin --username admin --email demo@gmail.com --firstname admin --lastname admin --password admin

# run webserver
airflow webserver -p 8080

# run scheduler
airflow scheduler
```