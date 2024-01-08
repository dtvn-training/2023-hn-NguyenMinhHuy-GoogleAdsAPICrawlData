version = 15

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
f"""
CREATE TABLE IF NOT EXISTS resources (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(60) NOT NULL UNIQUE,
    description TEXT NOT NULL,
    with_metrics TINYINT(1) NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT {version}
);
""",
f"""
CREATE TABLE IF NOT EXISTS fields (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT NOT NULL,
    category VARCHAR(255) NOT NULL,
    type_url TEXT,
    filterable VARCHAR(10) NOT NULL,
    selectable VARCHAR(10) NOT NULL,
    sortable VARCHAR(10) NOT NULL,
    repeated VARCHAR(10) NOT NULL,
    version INT NOT NULL DEFAULT {version}
);
"""
,
f"""
CREATE TABLE IF NOT EXISTS segmenting_attributed_resources (
    resource_id_1 INT NOT NULL,
    resource_id_2 INT NOT NULL,
    type VARCHAR(45) NOT NULL DEFAULT 'attributed_resources',
    version INT NOT NULL DEFAULT {version},
    PRIMARY KEY (resource_id_1, resource_id_2, type),
    FOREIGN KEY (resource_id_1) REFERENCES resources(id),
    FOREIGN KEY (resource_id_2) REFERENCES resources(id)
);
"""

,
f"""
CREATE TABLE IF NOT EXISTS resources_fields (
    resources_id INT NOT NULL,
    fields_id INT NOT NULL,
    version INT NOT NULL DEFAULT {version},
    PRIMARY KEY (resources_id, fields_id),
    FOREIGN KEY (resources_id) REFERENCES resources(id),
    FOREIGN KEY (fields_id) REFERENCES fields(id)
);
"""
,
f"""
CREATE TABLE IF NOT EXISTS data_type (
    field_id INT NOT NULL,
    name VARCHAR(200) NOT NULL,
    enum_value VARCHAR(200) DEFAULT "NULL",
    version INT NOT NULL DEFAULT {version},
    PRIMARY KEY (field_id, name, enum_value),
    FOREIGN KEY (field_id) REFERENCES fields(id)
);
"""
,
f"""
CREATE TABLE IF NOT EXISTS selectable_with (
    field_id INT NOT NULL,
    selectable_name VARCHAR(200) NOT NULL,
    selectable_type VARCHAR(45) NOT NULL,
    version INT NOT NULL DEFAULT {version},
    PRIMARY KEY (field_id, selectable_name),
    FOREIGN KEY (field_id) REFERENCES fields(id)
);
"""
]