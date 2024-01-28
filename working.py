import json
import mysql.connector

# Replace these with your actual database and JSON file paths
ideal_data_file = 'C:/Users/Sejal/Desktop/CRON/ideal_data.json'
mysql_user = 'root'
mysql_password = 'root'
mysql_host = 'localhost'
mysql_port = '3306'
mysql_db = 'WUPOS'

def load_ideal_data(file_path):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)

def connect_to_database():
    # Connecting to MySQL
    mysql_connection = mysql.connector.connect(
        user=mysql_user,
        password=mysql_password,
        host=mysql_host,
        port=mysql_port,
        database=mysql_db
    )
    print("Connected to MySQL database successfully.")

    return mysql_connection
def compare_and_update(mysql_connection, ideal_data):
    cursor = mysql_connection.cursor(dictionary=True)

    # Mapping of table names to their unique ID columns
    table_unique_id_columns = {
        "agent": "agent_id",
        "agent_profile": "agent_profile_id",
        "terminal": "terminal_id",

    }

    for table_name, rows in ideal_data.items():
        unique_id_column = table_unique_id_columns.get(table_name)
        if unique_id_column is None:
            print(f"Unique ID column not specified for table {table_name}")
            continue

        for unique_id_value, ideal_row in rows.items():
            if not isinstance(ideal_row, dict):
                print(f"Unexpected format for ideal_row in table {table_name}")
                continue

            # Use the specified unique ID column for the table
            unique_id_value = ideal_row.get(unique_id_column)
            update_values = {key: value for key, value in ideal_row.items() if key != unique_id_column}

            # Check the username condition in agent_user table
            cursor.execute("SELECT * FROM agent_user WHERE agent_id = %s", (unique_id_value,))
            agent_user_data = cursor.fetchone()

            if agent_user_data and agent_user_data['USERNAME'].startswith('TIMEX'):
                # Continue with the update
                update_query = (
                    f"UPDATE {table_name} "
                    f"SET {', '.join([f'{col} = %s' for col in update_values.keys()])} "
                    f"WHERE {unique_id_column} = %s"
                )

                try:
                    cursor.execute(update_query, list(update_values.values()) + [unique_id_value])
                    mysql_connection.commit()
                    print(f"Update successful for {table_name} with {unique_id_column}={unique_id_value}")
                except mysql.connector.Error as update_error:
                    print(f"Update failed for {table_name} with {unique_id_column}={unique_id_value}: {update_error}")
            else:
                print(f"Skipping update for {table_name} with {unique_id_column}={unique_id_value} due to username condition")

    cursor.close()



if __name__ == "__main__":
    ideal_data = load_ideal_data(ideal_data_file)
    mysql_connection = connect_to_database()

    compare_and_update(mysql_connection, ideal_data)

    mysql_connection.close()
