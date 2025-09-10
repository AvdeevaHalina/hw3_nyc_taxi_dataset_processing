#!/bin/bash

cd /Users/Halina_Avdeeva/PycharmProjects/PythonProject/hw_fifa_dbt

source /Users/Halina_Avdeeva/PycharmProjects/PythonProject/.venv

dbt run

dbt test

if [ $? -eq 0 ]; then
    echo "DBT Job succeeded" | mail -s "DBT Job Status: Success" avdeevagalya@gmail.com
else
    echo "DBT Job failed" | mail -s "DBT Job Status: Failed" avdeevagalya@gmail.com
fi