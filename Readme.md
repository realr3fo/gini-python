# Gini-based Knowledge Imbalance Analysis on Wikidata

This project is created in order to fulfill 
the implementation of the project 
undergraduate thesis in the Faculty of Computer Science,
Universitas Indonesia. 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.


### Prerequisites

- Linux or Unix machine with root access
- Python 3 - [Python 3 installation](https://realpython.com/installing-python/)
- PostgreSQL - [PostgreSQL installation](https://www.2ndquadrant.com/en/blog/pginstaller-install-postgresql/)
```
git clone https://github.com/realr3fo/gini-python
```

### Installing

A step by step series of examples that tell you how to get a development env running

Make sure you are in the root folder of the project
1. install the dependencies
    ```
    pip3 install -r requirements.txt
    ```
2. Copy environment variables
    ```
    cp .env.example .env
    ```
3. Modify environment variable to fit database spec
4. Set up database
    ```
    CREATE DATABASE python_gini;
    ```
5. Create tables into the database
    ```
    python manage.py db init
    python manage.py db migrate
    python manage.py db upgrade
    ```
6. Run the project
    ```
    python manage.py runserver
    ```
Your server should be running in http://localhost:5000


## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Flask](https://flask-doc.readthedocs.io/en/latest/) - The web framework used
* [SQLAlchemy](https://docs.sqlalchemy.org/en/13/) - Database Management
* [AioSPARQL](https://github.com/aio-libs/aiosparql) - Used to get data from Wikidata asynchronously

## Documentation

We use the following [Apiary](https://prowd.docs.apiary.io/) for documentation.  

## Authors

**Refo Ilmiya** - *Initial work* - [realr3fo](https://github.com/realr3fo)
