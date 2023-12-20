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
metrics_file_path = 'google_ads_api/processed/metric.csv'
segments_file_path = 'google_ads_api/processed/segment.csv'
attributes_file_path = 'google_ads_api/processed/attribute.csv'


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
        DROP TABLE IF EXISTS segments CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS metrics CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS attributes CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS resources_attributes CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS resources_metrics CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS resources_segments CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS attributed_resources CASCADE;
        """
        ,
        """
        DROP TABLE IF EXISTS segmenting_resources CASCADE;
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
        CREATE TABLE IF NOT EXISTS metrics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            description TEXT NOT NULL,
            category VARCHAR(255) NOT NULL,
            data_type VARCHAR(255) NOT NULL,
            type_url TEXT NOT NULL,
            filterable VARCHAR(10) NOT NULL,
            selectable VARCHAR(10) NOT NULL,
            sortable VARCHAR(10) NOT NULL,
            repeated VARCHAR(10) NOT NULL
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS segments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            description TEXT NOT NULL,
            category VARCHAR(255) NOT NULL,
            data_type VARCHAR(255) NOT NULL,
            type_url TEXT NOT NULL,
            filterable VARCHAR(10) NOT NULL,
            selectable VARCHAR(10) NOT NULL,
            sortable VARCHAR(10) NOT NULL,
            repeated VARCHAR(10) NOT NULL
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS attributes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            description TEXT NOT NULL,
            category VARCHAR(255) NOT NULL,
            data_type VARCHAR(255) NOT NULL,
            type_url TEXT NOT NULL,
            filterable VARCHAR(10) NOT NULL,
            selectable VARCHAR(10) NOT NULL,
            sortable VARCHAR(10) NOT NULL,
            repeated VARCHAR(10) NOT NULL
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS attributed_resources (
            resource_id_1 INT NOT NULL,
            resource_id_2 INT NOT NULL,
            PRIMARY KEY (resource_id_1, resource_id_2),
            FOREIGN KEY (resource_id_1) REFERENCES resources(id),
            FOREIGN KEY (resource_id_2) REFERENCES resources(id)
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS segmenting_resources (
            resource_id_1 INT NOT NULL,
            resource_id_2 INT NOT NULL,
            PRIMARY KEY (resource_id_1, resource_id_2),
            FOREIGN KEY (resource_id_1) REFERENCES resources(id),
            FOREIGN KEY (resource_id_2) REFERENCES resources(id)
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS resources_attributes (
            resources_id INT NOT NULL,
            attributes_id INT NOT NULL,
            PRIMARY KEY (resources_id, attributes_id),
            FOREIGN KEY (resources_id) REFERENCES resources(id),
            FOREIGN KEY (attributes_id) REFERENCES attributes(id)
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS resources_metrics (
            resources_id INT NOT NULL,
            metrics_id INT NOT NULL,
            PRIMARY KEY (resources_id, metrics_id),
            FOREIGN KEY (resources_id) REFERENCES resources(id),
            FOREIGN KEY (metrics_id) REFERENCES metrics(id)
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS resources_segments (
            resources_id INT NOT NULL,
            segments_id INT NOT NULL,
            PRIMARY KEY (resources_id, segments_id),
            FOREIGN KEY (resources_id) REFERENCES resources(id),
            FOREIGN KEY (segments_id) REFERENCES segments(id)
        );
        """
        ]
        for query in create_table_query:
            cursor.execute(query)
        # cursor.execute(create_table_query)
        # cursor.close()
        # connection.commit()
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

            elif (table_name == 'metrics'):
                # Insert into metrics table
                insert_query_metrics = """
                INSERT INTO metrics (name, description, category, data_type, type_url, filterable, selectable, sortable, repeated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                data_metrics = (
                    row['metric_name'], row['metric_field_description'], row['metric_category'], 
                    row['metric_data_type'], row['metric_type_url'], row['metric_filterable'], 
                    row['metric_selectable'], row['metric_sortable'], row['metric_repeated'],
                )
                cursor.execute(insert_query_metrics, data_metrics)

            elif (table_name == 'segments'):
                # Insert into segments table
                insert_query_segments = """
                INSERT INTO segments (name, description, category, data_type, type_url, filterable, selectable, sortable, repeated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                data_segments = (
                    row['segment_name'], row['segment_field_description'], row['segment_category'],
                    row['segment_data_type'], row['segment_type_url'], row['segment_filterable'],
                    row['segment_selectable'], row['segment_sortable'], row['segment_repeated'],
                )
                cursor.execute(insert_query_segments, data_segments)

            elif (table_name == 'attributes'):
                # Insert into attributes table
                insert_query_attributes = """
                INSERT INTO attributes (name, description, category, data_type, type_url, filterable, selectable, sortable, repeated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                data_attributes = (
                    row['attribute_name'], row['attribute_field_description'], row['attribute_category'],
                    row['attribute_data_type'], row['attribute_type_url'], row['attribute_filterable'],
                    row['attribute_selectable'], row['attribute_sortable'], row['attribute_repeated'],
                )
                cursor.execute(insert_query_attributes, data_attributes)
            
            elif table_name == 'attributed_resources':
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
                                INSERT INTO attributed_resources (resource_id_1, resource_id_2)
                                VALUES (%s, %s);
                                """
                                data_attributed_resources = (resource_id_1, resource_id_2)
                                cursor.execute(insert_query_attributed_resources, data_attributed_resources)
                            else:
                                print(f"Resource '{attributed_resource}' not found.")
                else:
                    print(f"Resource '{row['resource_name']}' not found.")

            elif table_name == 'segmenting_resources':
                # Assuming 'resource_name' is a column in your DataFrame
                # Retrieve resource_id_1 based on 'resource_name'
                cursor.execute("SELECT id FROM resources WHERE name = %s", (row['resource_name'],))
                result = cursor.fetchone()

                # Check if a row is returned
                if result:
                    resource_id_1 = result[0]

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
                                INSERT INTO segmenting_resources (resource_id_1, resource_id_2)
                                VALUES (%s, %s);
                                """
                                data_segmenting_resources = (resource_id_1, resource_id_2)
                                cursor.execute(insert_query_segmenting_resources, data_segmenting_resources)
                            else:
                                print(f"Resource '{segmenting_resource}' not found.")
                else:
                    print(f"Resource '{row['resource_name']}' not found.")

            elif table_name == 'resources_attributes':
                # Assuming 'resource_name' is a column in your DataFrame
                # Retrieve resources_id based on 'resource_name'
                cursor.execute("SELECT id FROM resources WHERE name = %s", (row['resource_name'],))
                result = cursor.fetchone()

                # Check if a row is returned
                if result:
                    resources_id = result[0] # resources_id is the id of the resource

                    # Assuming 'attributes' is a column containing a list of attributes
                    attributes = row['list_attributes'].split(',')

                    # Insert pairs into attrbutes table
                    for attribute in attributes:
                        # Retrieve attribute_id based on 'attribute'
                        if attribute: # Check if attribute is not empty
                            # if attribute.strip() does not already have the resource name, add it
                            check_resource_name = row['resource_name'] + "." 
                            if attribute.strip().find(check_resource_name) == -1:  
                                attribute = row['resource_name'] + "." + attribute.strip()
                                cursor.execute("SELECT id FROM attributes WHERE name = %s", (attribute,))
                            else:
                                cursor.execute("SELECT id FROM attributes WHERE name = %s", (attribute.strip(),))
                            result = cursor.fetchone()

                            # Check if a row is returned
                            if result:
                                attributes_id = result[0]
                                # Insert into attributes table
                                insert_query_resources_attributes = """
                                INSERT INTO resources_attributes (resources_id, attributes_id)
                                VALUES (%s, %s);
                                """
                                data_resources_attributes = (resources_id, attributes_id)
                                cursor.execute(insert_query_resources_attributes, data_resources_attributes)
                            else:
                                print(f"Resource '{attribute}' not found.")
                else:
                    print(f"Resource '{row['resource_name']}' not found.")


            elif table_name == 'resources_metrics':
                # Assuming 'resource_name' is a column in your DataFrame
                # Retrieve resources_id based on 'resource_name'
                cursor.execute("SELECT id FROM resources WHERE name = %s", (row['resource_name'],))
                result = cursor.fetchone()

                # Check if a row is returned
                if result:
                    resources_id = result[0] # resources_id is the id of the resource

                    # Assuming 'metrics' is a column containing a list of metrics
                    metrics = row['list_metrics'].split(',')

                    # Insert pairs into metrics table
                    for metric in metrics:
                        # Retrieve metrics_id based on 'metric'
                        if metric: # Check if metric is not empty
                            # add 'metrics.' to the beginning of the metric name
                            metric = "metrics." + metric.strip()
                            cursor.execute("SELECT id FROM metrics WHERE name = %s", (metric,))
                            result = cursor.fetchone()

                            # Check if a row is returned
                            if result:
                                metrics_id = result[0]
                                # Insert into metrics table
                                insert_query_resources_metrics = """
                                INSERT INTO resources_metrics (resources_id, metrics_id)
                                VALUES (%s, %s);
                                """
                                data_resources_metrics = (resources_id, metrics_id)
                                cursor.execute(insert_query_resources_metrics, data_resources_metrics)
                            else:
                                print(f"Resource '{metric}' not found.")
                else:
                    print(f"Resource '{row['resource_name']}' not found.")

            elif table_name == 'resources_segments':
                # Assuming 'resource_name' is a column in your DataFrame
                # Retrieve resources_id based on 'resource_name'
                cursor.execute("SELECT id FROM resources WHERE name = %s", (row['resource_name'],))
                result = cursor.fetchone()

                # Check if a row is returned
                if result:
                    resources_id = result[0] # resources_id is the id of the resource

                    # Assuming 'segments' is a column containing a list of segments 
                    segments = row['list_segments'].split(',')

                    # Insert pairs into segments table
                    for segment in segments:
                        # Retrieve segments_id based on 'segment'
                        if segment: # Check if segment is not empty
                            # add 'segments.' to the beginning of the segment name
                            segment = "segments." + segment.strip()
                            cursor.execute("SELECT id FROM segments WHERE name = %s", (segment,))
                            result = cursor.fetchone()

                            # Check if a row is returned
                            if result:
                                segments_id = result[0]
                                # Insert into segments table
                                insert_query_resources_segments = """
                                INSERT INTO resources_segments (resources_id, segments_id)
                                VALUES (%s, %s);
                                """
                                data_resources_segments = (resources_id, segments_id)
                                cursor.execute(insert_query_resources_segments, data_resources_segments)
                            else:
                                print(f"Resource '{segment}' not found.")
                else:
                    print(f"Resource '{row['resource_name']}' not found.")
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

load_csv_to_mysql(metrics_file_path, 'metrics', db_config)

load_csv_to_mysql(segments_file_path, 'segments', db_config)

load_csv_to_mysql(attributes_file_path, 'attributes', db_config)

load_csv_to_mysql(resources_file_path, 'attributed_resources', db_config)

load_csv_to_mysql(resources_file_path, 'segmenting_resources', db_config)

load_csv_to_mysql(resources_file_path, 'resources_attributes', db_config)

load_csv_to_mysql(resources_file_path, 'resources_metrics', db_config)

load_csv_to_mysql(resources_file_path, 'resources_segments', db_config)


