import pandas as pd
import mysql.connector
from mysql.connector import Error
from config import db_config, resources_file_path14, fields_file_path14
# from queries14 import create_table_query

# def create_table(db_config): # this function create table in MySQL database
#     try:
#         # Establish MySQL database connection
#         connection = mysql.connector.connect(**db_config)

#         # Create a MySQL cursor
#         cursor = connection.cursor()

#         for query in create_table_query:
#             cursor.execute(query)
#         print("Table created successfully in MySQL database.")

#     except Error as e:
#         print(f"Error: {e}")

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
                INSERT INTO resources (name, description, with_metrics, version)
                VALUES (%s, %s, %s, "14");
                """
                data_resources = (row['resource_name'], row['description'], row['with_metrics'])
                cursor.execute(insert_query_resources, data_resources)

            elif (table_name == 'fields'):
                # Insert into fields table
                insert_query_data_type = """
                INSERT INTO fields (name, description, category, type_url, filterable, selectable, sortable, repeated, version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "14");
                """
                data_segments = (
                    row['field_name'], row['field_field_description'], row['field_category'],
                    row['field_type_url'], row['field_filterable'],
                    row['field_selectable'], row['field_sortable'], row['field_repeated']
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
                                INSERT INTO segmenting_attributed_resources (resource_id_1, resource_id_2, type, version)
                                VALUES (%s, %s, "attributed_resources", "14");
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
                                INSERT INTO segmenting_attributed_resources (resource_id_1, resource_id_2, type, version)
                                VALUES (%s, %s, "segmenting_resources", "14");
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
                                INSERT INTO resources_fields (resources_id, fields_id, version)
                                VALUES (%s, %s, "14");
                                """
                                data_resources_attributes = (resources_id, attributes_id)
                                cursor.execute(insert_query_resources_attributes, data_resources_attributes)
                            else:
                                print(f"Attribute '{attribute}' not found.")

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
                                INSERT INTO resources_fields (resources_id, fields_id, version)
                                VALUES (%s, %s, "14");
                                """
                                data_resources_metrics = (resources_id, metrics_id)
                                cursor.execute(insert_query_resources_metrics, data_resources_metrics)
                            else:
                                print(f"Metric '{metric}' not found.")


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
                                INSERT INTO resources_fields (resources_id, fields_id, version)
                                VALUES (%s, %s, "14");
                                """
                                data_resources_segments = (resources_id, segments_id)
                                cursor.execute(insert_query_resources_segments, data_resources_segments)
                            else:
                                print(f"Segment '{segment}' not found.")

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
                    # if data_type_names have more than one element, insert each element into data_type table
                    if len(data_type_names) == 1: # if data_type_names only have one element, insert it into data_type table
                        for data_type_name in data_type_names:
                            # Retrieve resource_id_2 based on 'attributed_resource'
                            if data_type_name: # Check if attributed_resource is not empty
                                # if data_type_name.strip() 
                                insert_query_attributed_resources = """
                                INSERT INTO data_type (field_id, name, version)
                                VALUES (%s, %s, "14");
                                """
                                data_attributed_resources = (field_id, data_type_name.strip())
                                cursor.execute(insert_query_attributed_resources, data_attributed_resources)
                            else:
                                print(f"Attributed resource '{attributed_resource}' not found.")
                    else: # if data_type_names have more than one element, insert each element into data_type table as ENUM
                        for data_type_name in data_type_names:
                            # Retrieve resource_id_2 based on 'attributed_resource'
                            if data_type_name: # Check if attributed_resource is not empty
                                # if data_type_name.strip() 
                                insert_query_attributed_resources = """
                                INSERT INTO data_type (field_id, name, enum_value, version)
                                VALUES (%s, %s, %s, "14");
                                """
                                data_attributed_resources = (field_id, "ENUM", data_type_name.strip())
                                cursor.execute(insert_query_attributed_resources, data_attributed_resources)
                            else:
                                # print(f"Attributed resource '{attributed_resource}' not found.")
                                print(f"Field '{row['field_name']}' not found.")


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
                            INSERT INTO selectable_with (field_id, selectable_name, selectable_type, version)
                            VALUES (%s, %s, %s, "14");
                            """
                            data_attributed_resources = (field_id, selectable_name.strip(), selectable_type)
                            cursor.execute(insert_query_attributed_resources, data_attributed_resources)
                else:
                    # print(f"Attributed resource '{attributed_resource}' not found.")
                    print(f"Field '{row['field_name']}' not found.")

            else:
                print("Table name not found.")

            # Commit changes and close the connection
        connection.commit()
        cursor.close()
        connection.close()

        print("Data loaded successfully to MySQL database.")
        print("Finished loading data to MySQL database.")

    except Error as e:
        print(f"Error: {e}")

# Create tables in the database if they don't exist
# create_table(db_config)
 
load_csv_to_mysql(resources_file_path14, 'resources', db_config)

load_csv_to_mysql(fields_file_path14, 'fields', db_config)

load_csv_to_mysql(resources_file_path14, 'segmenting_attributed_resources', db_config)

load_csv_to_mysql(resources_file_path14, 'resources_fields', db_config)

load_csv_to_mysql(fields_file_path14, 'data_type', db_config)

load_csv_to_mysql(fields_file_path14, 'selectable_with', db_config)




