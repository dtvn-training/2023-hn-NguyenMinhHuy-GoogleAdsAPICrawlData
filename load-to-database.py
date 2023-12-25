import pandas as pd
import mysql.connector
from mysql.connector import Error

# MySQL database connection configuration
db_config = {
    'host': 'localhost',
    'database': 'googleadsapi',
    'user': 'test',
    'password': 'pass123',
}

# CSV file paths
resources_file_path = 'google_ads_api/processed/resource-process.csv'
fields_file_path = 'google_ads_api/processed/field.csv'


def create_table(db_config): # this function create table in mysql database
    try:
        # Establish MySQL database connection
        connection = mysql.connector.connect(**db_config)

        # Create a MySQL cursor
        cursor = connection.cursor()

        # Create a table in the database
        create_table_query = [
        """
        SET FOREIGN_KEY_CHECKS = 0;
        """
        ,
        """
        DROP TABLE IF EXISTS resources CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS fields CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS resources_fields CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS segmenting_attributed_resources CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS data_type CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS selectable_with CASCADE;
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS resources (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(60) NOT NULL UNIQUE,
            description TEXT NOT NULL,
            with_metrics TINYINT(1) NOT NULL DEFAULT 0
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fields (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            description TEXT NOT NULL,
            category VARCHAR(255) NOT NULL,
            type_url TEXT NOT NULL,
            filterable VARCHAR(10) NOT NULL,
            selectable VARCHAR(10) NOT NULL,
            sortable VARCHAR(10) NOT NULL,
            repeated VARCHAR(10) NOT NULL
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS segmenting_attributed_resources (
            resource_id_1 INT NOT NULL,
            resource_id_2 INT NOT NULL,
            type VARCHAR(45) NOT NULL DEFAULT 'attributed_resources',
            PRIMARY KEY (resource_id_1, resource_id_2, type),
            FOREIGN KEY (resource_id_1) REFERENCES resources(id),
            FOREIGN KEY (resource_id_2) REFERENCES resources(id)
        );
        """

        ,
        """
        CREATE TABLE IF NOT EXISTS resources_fields (
            resources_id INT NOT NULL,
            fields_id INT NOT NULL,
            PRIMARY KEY (resources_id, fields_id),
            FOREIGN KEY (resources_id) REFERENCES resources(id),
            FOREIGN KEY (fields_id) REFERENCES fields(id)
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS data_type (
            field_id INT NOT NULL,
            name VARCHAR(200) NOT NULL,
            PRIMARY KEY (field_id, name),
            FOREIGN KEY (field_id) REFERENCES fields(id)
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS selectable_with (
            field_id INT NOT NULL,
            selectable_name VARCHAR(200) NOT NULL,
            selectable_type VARCHAR(45) NOT NULL,
            PRIMARY KEY (field_id, selectable_name),
            FOREIGN KEY (field_id) REFERENCES fields(id)
        );
        """
        ]
        for query in create_table_query:
            cursor.execute(query)
        print("Table created successfully in MySQL database.")

    except Error as e:
        print(f"Error: {e}")

def load_csv_to_mysql(csv_file, table_name, db_config):     # This function load csv to mysql database 

    try:
        # Read CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)

        # Handle missing values (replace NaN with appropriate values)
        df = df.fillna('')  # Replace NaN with an empty string or another suitable value

        # Establish MySQL database connection
        connection = mysql.connector.connect(**db_config)

        # Create a MySQL cursor
        cursor = connection.cursor()
        for _, row in df.iterrows(): # Iterate over DataFrame rows as (index, Series) pairs.
            if (table_name == 'resources'):
                # Insert into resources table
                insert_query_resources = """
                INSERT INTO resources (name, description, with_metrics)
                VALUES (%s, %s, %s);
                """
                data_resources = (row['resource_name'], row['description'], row['with_metrics'],)
                cursor.execute(insert_query_resources, data_resources)

            elif (table_name == 'fields'):
                # Insert into fields table
                insert_query_data_type = """
                INSERT INTO fields (name, description, category, type_url, filterable, selectable, sortable, repeated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                data_segments = (
                    row['field_name'], row['field_field_description'], row['field_category'],
                    row['field_type_url'], row['field_filterable'],
                    row['field_selectable'], row['field_sortable'], row['field_repeated'],
                )
                cursor.execute(insert_query_data_type, data_segments)

            
            elif table_name == 'segmenting_attributed_resources':
                # Assuming 'resource_name' is a column in your DataFrame
                # Retrieve resource_id_1 based on 'resource_name'
                cursor.execute("SELECT id FROM resources WHERE name = %s", (row['resource_name'],))
                result = cursor.fetchone() 

                # Check if a row is returned
                if result:
                    resource_id_1 = result[0]

                    # Assuming 'attributed_resource' is a column containing a list of attributed resources
                    attributed_resources = row['attributed_resource'].split(',')

                    # Insert pairs into attributed_resources table
                    for attributed_resource in attributed_resources:
                        # Retrieve resource_id_2 based on 'attributed_resource'
                        if attributed_resource: # Check if attributed_resource is not empty
                            cursor.execute("SELECT id FROM resources WHERE name = %s", (attributed_resource.strip(),))
                            result = cursor.fetchone()

                            # Check if a row is returned
                            if result:
                                resource_id_2 = result[0]

                                # Insert into attributed_resources table
                                insert_query_attributed_resources = """
                                INSERT INTO segmenting_attributed_resources (resource_id_1, resource_id_2, type)
                                VALUES (%s, %s, "attributed_resources");
                                """
                                data_attributed_resources = (resource_id_1, resource_id_2)
                                cursor.execute(insert_query_attributed_resources, data_attributed_resources)
                            else:
                                print(f"Resource '{attributed_resource}' not found.")

                    # Assuming 'segmenting_resources' is a column containing a list of segmenting resources
                    segmenting_resources = row['segmenting_resource'].split(',')

                    # Insert pairs into segmenting_resources table
                    for segmenting_resource in segmenting_resources:
                        # Retrieve resource_id_2 based on 'segmenting_resource'
                        if segmenting_resource: # Check if segmenting_resource is not empty
                            cursor.execute("SELECT id FROM resources WHERE name = %s", (segmenting_resource.strip(),))
                            result = cursor.fetchone()

                            # Check if a row is returned
                            if result:
                                resource_id_2 = result[0]

                                # Insert into segmenting_resources table
                                insert_query_segmenting_resources = """
                                INSERT INTO segmenting_attributed_resources (resource_id_1, resource_id_2, type)
                                VALUES (%s, %s, "segmenting_resources");
                                """
                                data_segmenting_resources = (resource_id_1, resource_id_2)
                                cursor.execute(insert_query_segmenting_resources, data_segmenting_resources)
                            else:
                                print(f"Resource '{segmenting_resource}' not found.")
                else:
                    print(f"Resource '{row['resource_name']}' not found.")

            elif table_name == 'resources_fields':
                # Assuming 'resource_name' is a column in your DataFrame
                # Retrieve resources_id based on 'resource_name'
                cursor.execute("SELECT id FROM resources WHERE name = %s", (row['resource_name'],))
                result = cursor.fetchone()

                # Check if a row is returned
                if result:
                    resources_id = result[0] # resources_id is the id of the resource

                    # Assuming 'attributes' is a column containing a list of attributes
                    attributes = row['list_attributes'].split(',')

                    # Insert pairs into attributes table
                    for attribute in attributes:
                        # Retrieve attribute_id based on 'attribute'
                        if attribute: # Check if attribute is not empty
                            # if attribute.strip() does not already have the resource name, add it
                            check_resource_name = row['resource_name'] + "." 
                            if attribute.strip().find(check_resource_name) == -1:  
                                attribute = row['resource_name'] + "." + attribute.strip()
                                cursor.execute("SELECT id FROM fields WHERE name = %s", (attribute,))
                            else:
                                cursor.execute("SELECT id FROM fields WHERE name = %s", (attribute.strip(),))
                            result = cursor.fetchone()

                            # Check if a row is returned
                            if result:
                                attributes_id = result[0]
                                # Insert into attributes table
                                insert_query_resources_attributes = """
                                INSERT INTO resources_fields (resources_id, fields_id)
                                VALUES (%s, %s);
                                """
                                data_resources_attributes = (resources_id, attributes_id)
                                cursor.execute(insert_query_resources_attributes, data_resources_attributes)
                            else:
                                print(f"Resource '{attribute}' not found.")

                    # Assuming 'metrics' is a column containing a list of metrics
                    metrics = row['list_metrics'].split(',')

                    # Insert pairs into metrics table
                    for metric in metrics:
                        # Retrieve metrics_id based on 'metric'
                        if metric: # Check if metric is not empty
                            # add 'metrics.' to the beginning of the metric name
                            metric = "metrics." + metric.strip()
                            cursor.execute("SELECT id FROM fields WHERE name = %s", (metric,))
                            result = cursor.fetchone()

                            # Check if a row is returned
                            if result:
                                metrics_id = result[0]
                                # Insert into metrics table
                                insert_query_resources_metrics = """
                                INSERT INTO resources_fields (resources_id, fields_id)
                                VALUES (%s, %s);
                                """
                                data_resources_metrics = (resources_id, metrics_id)
                                cursor.execute(insert_query_resources_metrics, data_resources_metrics)
                            else:
                                print(f"Resource '{metric}' not found.")


                    # Assuming 'segments' is a column containing a list of segments 
                    segments = row['list_segments'].split(',')

                    # Insert pairs into segments table
                    for segment in segments:
                        # Retrieve segments_id based on 'segment'
                        if segment: # Check if segment is not empty
                            # add 'segments.' to the beginning of the segment name
                            segment = "segments." + segment.strip()
                            cursor.execute("SELECT id FROM fields WHERE name = %s", (segment,))
                            result = cursor.fetchone()

                            # Check if a row is returned
                            if result:
                                segments_id = result[0]
                                # Insert into segments table
                                insert_query_resources_segments = """
                                INSERT INTO resources_fields (resources_id, fields_id)
                                VALUES (%s, %s);
                                """
                                data_resources_segments = (resources_id, segments_id)
                                cursor.execute(insert_query_resources_segments, data_resources_segments)
                            else:
                                print(f"Resource '{segment}' not found.")

                else:
                    print(f"Resource '{row['resource_name']}' not found.")

            elif table_name == 'data_type':
                cursor.execute("SELECT id FROM fields WHERE name = %s", (row['field_name'],))
                result = cursor.fetchone() 

                # Check if a row is returned
                if result:
                    field_id = result[0] # field_id is the id of the field, the first column in the table

                    # Assuming 'attributed_resource' is a column containing a list of attributed resources
                    data_type_names = row['field_data_type'].split(',')

                    # Insert pairs into attributed_resources table
                    for data_type_name in data_type_names:
                        # Retrieve resource_id_2 based on 'attributed_resource'
                        if data_type_name: # Check if attributed_resource is not empty
                            insert_query_attributed_resources = """
                            INSERT INTO data_type (field_id, name)
                            VALUES (%s, %s);
                            """
                            data_attributed_resources = (field_id, data_type_name.strip())
                            cursor.execute(insert_query_attributed_resources, data_attributed_resources)
                        else:
                            print(f"Resource '{attributed_resource}' not found.")

            elif table_name == 'selectable_with':
                cursor.execute("SELECT id FROM fields WHERE name = %s", (row['field_name'],))
                result = cursor.fetchone() 

                # Check if a row is returned
                if result:
                    field_id = result[0] # field_id is the id of the field, the first column in the table

                    # Assuming 'attributed_resource' is a column containing a list of attributed resources
                    selectable_names = row['field_selectable_with'].split(',')

                    # Insert pairs into attributed_resources table
                    for selectable_name in selectable_names:
                        # Retrieve resource_id_2 based on 'attributed_resource'
                        if selectable_name: # Check if attributed_resource is not empty
                            if selectable_name.find("metrics.") != -1:
                                selectable_type = "metrics"
                            elif selectable_name.find("segments.") != -1:
                                selectable_type = "segments"
                            else:
                                selectable_type = "resources"
                            insert_query_attributed_resources = """
                            INSERT INTO selectable_with (field_id, selectable_name, selectable_type)
                            VALUES (%s, %s, %s);
                            """
                            data_attributed_resources = (field_id, selectable_name.strip(), selectable_type)
                            cursor.execute(insert_query_attributed_resources, data_attributed_resources)
                else:
                    print(f"Resource '{attributed_resource}' not found.")
            # elif table_name == 'resources_metrics':
            #     # Assuming 'resource_name' is a column in your DataFrame
            #     # Retrieve resources_id based on 'resource_name'
            #     cursor.execute("SELECT id FROM resources WHERE name = %s", (row['resource_name'],))
            #     result = cursor.fetchone()

            #     # Check if a row is returned
            #     if result:
            #         resources_id = result[0] # resources_id is the id of the resource

            #         # Assuming 'metrics' is a column containing a list of metrics
            #         metrics = row['list_metrics'].split(',')

            #         # Insert pairs into metrics table
            #         for metric in metrics:
            #             # Retrieve metrics_id based on 'metric'
            #             if metric: # Check if metric is not empty
            #                 # add 'metrics.' to the beginning of the metric name
            #                 metric = "metrics." + metric.strip()
            #                 cursor.execute("SELECT id FROM metrics WHERE name = %s", (metric,))
            #                 result = cursor.fetchone()

            #                 # Check if a row is returned
            #                 if result:
            #                     metrics_id = result[0]
            #                     # Insert into metrics table
            #                     insert_query_resources_metrics = """
            #                     INSERT INTO resources_metrics (resources_id, metrics_id)
            #                     VALUES (%s, %s);
            #                     """
            #                     data_resources_metrics = (resources_id, metrics_id)
            #                     cursor.execute(insert_query_resources_metrics, data_resources_metrics)
            #                 else:
            #                     print(f"Resource '{metric}' not found.")
            #     else:
            #         print(f"Resource '{row['resource_name']}' not found.")

            # elif table_name == 'resources_segments':
            #     # Assuming 'resource_name' is a column in your DataFrame
            #     # Retrieve resources_id based on 'resource_name'
            #     cursor.execute("SELECT id FROM resources WHERE name = %s", (row['resource_name'],))
            #     result = cursor.fetchone()

            #     # Check if a row is returned
            #     if result:
            #         resources_id = result[0] # resources_id is the id of the resource

            #         # Assuming 'segments' is a column containing a list of segments 
            #         segments = row['list_segments'].split(',')

            #         # Insert pairs into segments table
            #         for segment in segments:
            #             # Retrieve segments_id based on 'segment'
            #             if segment: # Check if segment is not empty
            #                 # add 'segments.' to the beginning of the segment name
            #                 segment = "segments." + segment.strip()
            #                 cursor.execute("SELECT id FROM segments WHERE name = %s", (segment,))
            #                 result = cursor.fetchone()

            #                 # Check if a row is returned
            #                 if result:
            #                     segments_id = result[0]
            #                     # Insert into segments table
            #                     insert_query_resources_segments = """
            #                     INSERT INTO resources_segments (resources_id, segments_id)
            #                     VALUES (%s, %s);
            #                     """
            #                     data_resources_segments = (resources_id, segments_id)
            #                     cursor.execute(insert_query_resources_segments, data_resources_segments)
            #                 else:
            #                     print(f"Resource '{segment}' not found.")
            #     else:
            #         print(f"Resource '{row['resource_name']}' not found.")
            else:
                print("Table name not found.")

            # Commit changes and close the connection
        connection.commit()
        cursor.close()
        connection.close()

        print("Data loaded successfully to MySQL database.")

    except Error as e:
        print(f"Error: {e}")

# Create tables in the database if they don't exist
create_table(db_config)

load_csv_to_mysql(resources_file_path, 'resources', db_config)

load_csv_to_mysql(fields_file_path, 'fields', db_config)

load_csv_to_mysql(resources_file_path, 'segmenting_attributed_resources', db_config)

load_csv_to_mysql(resources_file_path, 'resources_fields', db_config)

load_csv_to_mysql(fields_file_path, 'data_type', db_config)

load_csv_to_mysql(fields_file_path, 'selectable_with', db_config)




