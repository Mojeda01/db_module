import psycopg2

class Database:
    def __init__(self, dbname, user, password, host='localhost', port='5432'):
        self.connection = None 
        try:
            self.connection = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.cursor = self.connection.cursor()
            print(f"Connected to PostgreSQL and the new database! {dbname}")
        except Exception as e:
            print(f'Error connecting to the database: {e}')
    
    def create_table(self, table_name, schema):
        """Create a table with the given schema."""
        try:
            self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                {schema}
                );
            """)
            self.connection.commit()
            print(f'Table "{table_name}" created successfully.')
        except Exception as e:
            print(f'Error creating table "{table_name}": {e}')
    
    def fetch_tables(self):
        """Fetch and return all tables in the database."""
        try:
            self.cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public';
            """)
            return [table|[0] for table in self.cursor.fetchall()]
        except Exception as e:
            print(f'Error fetching tables: {e}')
            return []
    
    def delete_table(self, table_name):
        """Delete a table with the given name."""
        try:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            self.connection.commit()
            print(f"Table '{table_name}' deleted successfully.")
        except Exception as e:
            print(f"Error deleting table '{table_name}': {e}")
    
    def delete_database(self, dbname, username, password, host="localhost", port="5432"):
        """Delete a database with the given name."""
        try:
            # Close the existing connection before attempting to delete the database 
            self.close()

            # Connect to the default 'postgres' database to execute DROP DATABASE 
            connection = psycopg2.connect(
                dbname=dbname,
                user=username,
                password=password,
                host=host,
                port=port
            )
            cursor = connection.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS {dbname};")
            connection.commit()
            cursor.close()
            connection.close()
            print(f"Database '{dbname}' deleted successfully.")
        except Exception as e: 
            print(f"Error deleting database '{dbname}': {e}")
    
    def close(self):
        """Close the database connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print('Database connection closed.')
        except Exception as e:
            print(f'Error closing the database connection: {e}')