# import mysql.connector
# from config import db_config

# def compare_versions(db_config, table_name, key_field='name'):
#     try:
#         # Establish MySQL database connection
#         connection = mysql.connector.connect(**db_config)

#         # Create a MySQL cursor
#         cursor = connection.cursor(dictionary=True)

#         # Select data from the two versions
#         query_version_1 = f"SELECT * FROM {table_name} WHERE version = 14"
#         query_version_2 = f"SELECT * FROM {table_name} WHERE version = 15"

#         cursor.execute(query_version_1)
#         version_1_data = {row[key_field]: row for row in cursor.fetchall()}

#         cursor.execute(query_version_2)
#         version_2_data = {row[key_field]: row for row in cursor.fetchall()}

#         # Compare the two versions
#         differences = []
#         for key in version_1_data:
#             if key not in version_2_data:
#                 differences.append(f"Record with key '{key}' not found in version 2.")
#             else:
#                 for field, value_1 in version_1_data[key].items():
#                     value_2 = version_2_data[key][field]
#                     if value_1 != value_2:
#                         differences.append(f"Difference in record '{key}': Field '{field}' - Version 1: '{value_1}', Version 2: '{value_2}'")

#         # Output or log the differences
#         for diff in differences:
#             print(diff)

#     except Exception as e:
#         print(f"Error: {e}")

#     finally:
#         # Close the database connection
#         if connection.is_connected():
#             cursor.close()
#             connection.close()

# # Example usage
# # db_config = {
# #     'host': 'your_host',
# #     'user': 'your_user',
# #     'password': 'your_password',
# #     'database': 'your_database',
# # }

# # Replace 'your_table_name' with the actual table name you want to compare
# compare_versions(db_config, 'resources')
# compare_versions(db_config, 'fields')
# compare_versions(db_config, 'resources_fields')
# compare_versions(db_config, 'segmenting_attributed_resources')
# compare_versions(db_config, 'data_type')
# compare_versions(db_config, 'selectable_with')


# /////////////////////
import mysql.connector
from config import db_config
from datetime import datetime

def compare_versions(db_config, table_name, key_field='name'):
    try:
        # Establish MySQL database connection
        connection = mysql.connector.connect(**db_config)

        # Create a MySQL cursor
        cursor = connection.cursor(dictionary=True)  # Use dictionary=True to get results as dictionaries

        # Select data from the two versions
        query_version_1 = f"SELECT * FROM {table_name} WHERE version = 14"
        query_version_2 = f"SELECT * FROM {table_name} WHERE version = 15"

        cursor.execute(query_version_1)
        version_1_data = {row[key_field]: row for row in cursor.fetchall()}

        cursor.execute(query_version_2)
        version_2_data = {row[key_field]: row for row in cursor.fetchall()}

        # Get the current timestamp for the log file name
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        log_file_name = f"{table_name}_comparison_{timestamp}.log"

        # Compare the two versions and write differences to the log file
        with open(log_file_name, 'w') as log_file:
            differences = []
            for key in version_1_data:
                if key not in version_2_data:
                    differences.append(f"Record with key '{key}' not found in version 2.")
                else:
                    for field, value_1 in version_1_data[key].items():
                        value_2 = version_2_data[key][field]
                        if value_1 != value_2:
                            differences.append(f"Difference in record '{key}': Field '{field}' - Version 1: '{value_1}', Version 2: '{value_2}'")

            # Write differences to the log file
            for diff in differences:
                log_file.write(diff + '\n')

        print(f"Differences logged to {log_file_name}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the database connection
        if connection.is_connected():
            cursor.close()
            connection.close()

# Example usage
# db_config = {
#     'host': 'your_host',
#     'user': 'your_user',
#     'password': 'your_password',
#     'database': 'your_database',
# }

# Replace 'your_table_name' with the actual table name you want to compare
compare_versions(db_config, 'resources')
compare_versions(db_config, 'fields')
# compare_versions(db_config, 'resources_fields')
# compare_versions(db_config, 'segmenting_attributed_resources')
compare_versions(db_config, 'data_type')
# compare_versions(db_config, 'selectable_with')
