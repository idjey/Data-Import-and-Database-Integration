# Data-Import-and-Database-Integration

## Overview

This repository contains Python code for performing various data operations, including data extraction from CSV files, data transformation, and data insertion into a PostgreSQL database. The code also establishes an SSH tunnel connection to the database server for secure data transfer.

The code is designed to facilitate the import and storage of inventory data, specifically specimen and aliquot information, into a PostgreSQL database. Additionally, it supports the import of alternate IDs and general classifier data. The code ensures data consistency and correctness during the import process.

## Requirements

Before using the code, ensure that you have the following requirements met:

1. **Python 3.x**: You must have Python 3.x installed on your system.

2. **Python Libraries**: Install the required Python libraries using `pip install`:
   - `psycopg2`: PostgreSQL database adapter
   - `sshtunnel`: SSH tunneling library
   - `pandas`: Data manipulation library
   - `numpy`: Numerical computing library
   - `datetime`: Date and time manipulation library
   - `re`: Regular expressions library
   - `json`: JSON data format library
   - `os`: Operating system interaction library

3. **PostgreSQL Database**: Ensure you have access to a PostgreSQL database with appropriate permissions.

4. **Data Files**:
   - Prepare CSV files containing inventory data (specimen and aliquot information) in a specified directory.
   - An Excel file with alternate IDs data.
   - A CSV file with general classifier data.

## Code Structure

The code is structured as follows:

1. **Importing Libraries**: Import necessary Python libraries at the beginning of the code.

2. **Importing Inventories from CSV Files**: The `import_inventories` function reads CSV files, standardizes column names, and processes participant IDs, visit IDs, and other columns. It returns a concatenated DataFrame containing inventory data.

3. **Creating Tables in PostgreSQL Database**: The `create_tables` function defines SQL statements to create tables for storing specimen data in the PostgreSQL database. These tables include study, participant, visit, specimen, aliquot, location, lab, sample request, request batch, alt ID, and general classifier tables.

4. **Inserting Data into Tables**: Functions are provided to insert data into corresponding tables:
   - `insert_study`: Inserts study data into the study table.
   - `insert_participants`: Inserts participant data into the participant table.
   - `insert_visits`: Inserts visit data into the visit table.
   - `insert_specimens`: Inserts specimen data into the specimen table.
   - `insert_aliquots`: Inserts aliquot data into the aliquot table.
   - `insert_alt_ids`: Inserts alternate IDs data into the alt_id table.
   - `insert_general_classifiers`: Inserts general classifier data into the general_classifier table.

5. **SSH Tunnel Configuration**: SSH tunnel configuration parameters, including username and password, are specified to establish a secure connection to the PostgreSQL database server.

6. **Database Connection**: The code establishes a connection to the PostgreSQL database through an SSH tunnel using the `psycopg2` library. The connection details, including the host, port, username, password, and database name, are provided.

7. **Executing Database Operations**: You can uncomment and call the functions to perform specific database operations, such as importing specimens, aliquots, alternate IDs, or general classifier data. Provide the necessary data files as function arguments.

8. **Committing Changes**: After completing the database operations, the code commits the changes to the database.

## How to Use the Code

To use the code, follow these steps:

1. Make sure you have all the required Python libraries installed using `pip install`.

2. Configure the SSH tunnel settings by setting the `user_name_ssh` and `pw_ssh` variables with your SSH credentials.

3. Configure the PostgreSQL database connection settings by specifying the `hostname`, `username`, `password`, and `database` variables.

4. Ensure that you have the required data files (CSV, Excel) available for import.

5. Uncomment and call the functions you need to perform the desired database operations (e.g., importing specimens, aliquots, alternate IDs, or general classifier data). Provide the necessary data files as function arguments.

6. Run the code to execute the specified database operations and commit the changes.

## Conclusion

This Python code provides a structured approach to import and store inventory data into a PostgreSQL database while ensuring data consistency and correctness. It also establishes a secure SSH tunnel connection to the database server for data transfer. By following the provided instructions, you can customize and use this code to meet your specific data import requirements.
