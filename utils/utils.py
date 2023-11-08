import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


SCHEMAS = ['staging', 'data_mart']

def connect_to_database():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_ADDRESS'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None

def check_postgres_connection():
    connection = connect_to_database()
    if connection:
        connection.close()
        return True
    else:
        return False


def check_schemas(connection, schemas):
    missing_schemas = []
    try:
        cursor = connection.cursor()
        for schema in schemas:
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema,))
            if cursor.fetchone() is None:
                missing_schemas.append(schema)
        if missing_schemas:
            return False, missing_schemas       
        else:
            return True, [] 
    except Exception as e:
        print(f"Ошибка при проверке схемы: {e}")
        return False, schemas


def check_postgres_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_ADDRESS'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        conn.close()
        return True
    except psycopg2.OperationalError:
        return False


def main():
    if check_postgres_connection():
        print("Соединение с PostgreSQL установлено.")
        SCHEMAS = ['staging', 'data_mart']
        conn = connect_to_database()
        if conn:
            success, missing_schemas = check_schemas(connection=conn, schemas=SCHEMAS)
            if success:
                print("Все схемы существуют в базе данных.")
            else:
                print(f"Следующие схемы отсутствуют в базе данных: {', '.join(missing_schemas)}")
            conn.close()
    else:
        print("Невозможно установить соединение с PostgreSQL.")


if __name__ == "__main__":
    main()


#def column_name_is_exist(schema_name, column_name):
#    pass
