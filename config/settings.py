DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'flight_data',
    'user': 'your_username',
    'password': 'your_password'
}

ETL_CONFIG = {
    'input_directory': 'data/input',
    'output_directory': 'data/output',
    'log_file': 'logs/etl.log'
}

LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'logs/app.log'
        },
        'console': {
            'class': 'logging.StreamHandler'
        }
    }
}