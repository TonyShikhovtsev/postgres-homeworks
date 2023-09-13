import psycopg2
import csv

def main():
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="Sql481516",
            dbname="north",
        )
        cursor = conn.cursor()

        # Загрузка данных в таблицу employees
        with open('north_data/employees_data.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                cursor.execute(
                    "INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    row
                )

        # Загрузка данных в таблицу customers
        with open('north_data/customers_data.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                cursor.execute(
                    "INSERT INTO customers (customer_id, company_name, contact_name) "
                    "VALUES (%s, %s, %s)",
                    row
                )

        # Загрузка данных в таблицу orders
        with open('north_data/orders_data.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                cursor.execute(
                    "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    row
                )

        # Сохранение изменений и закрытие соединения
        conn.commit()
        print("Данные успешно загружены в таблицы.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
