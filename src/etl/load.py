def load_data_to_database(transformed_data, db_connection):
    """
    Load transformed data into the reconciled database and data warehouse.
    
    Parameters:
    transformed_data (list of dict): The data to be loaded.
    db_connection (object): The database connection object.
    """
    try:
        with db_connection.cursor() as cursor:
            for record in transformed_data:
                # Assuming record is a dictionary with keys matching the database columns
                cursor.execute("""
                    INSERT INTO your_table_name (column1, column2, column3)
                    VALUES (%s, %s, %s)
                """, (record['column1'], record['column2'], record['column3']))
            db_connection.commit()
    except Exception as e:
        db_connection.rollback()
        print(f"An error occurred while loading data: {e}")