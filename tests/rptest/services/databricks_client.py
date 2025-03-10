# Copyright 2024 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import logging
from typing import List, Optional, Any
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession


class DatabricksClient:
    """
    A client for managing Databricks Catalog objects used in Iceberg tests.
    
    This client provides functions to manage external locations, catalogs, tables,
    and to run SQL queries against data stored in hosted Databricks service.
    
    Attributes:
        catalog_name (str): The default catalog name to be used.
        storage_root (str): The default storage root for catalog data.
        workspace_client (WorkspaceClient): The client used for interacting with the Databricks API.
        logger (logging.Logger): Logger for debug and error messages.
    """
    def __init__(self, catalog_name: str, storage_root: str):
        """
        Initialize a new instance of DatabricksClient.
        
        Sets up connection details and verifies that the required
        environment variables (DATABRICKS_HOST and DATABRICKS_TOKEN) are set.

        Args:
            catalog_name (str): Catalog name to be used.
            storage_root (str): The root location in cloud storage for catalog data. (assumes that all IAM policies are in place)
            
        Raises:
            ValueError: If the required environment variables are not set.
        """
        # Check that required environment variables are set.
        required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
        missing = [
            var for var in required_vars
            if var not in os.environ or not os.environ[var]
        ]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        self.catalog_name = catalog_name
        self.storage_root = storage_root
        self.logger = logging.getLogger("DatabricksService")
        self.workspace_client = WorkspaceClient()
        self.logger.setLevel(logging.DEBUG)

    # External locations Management
    def create_external_location(self,
                                 name: str,
                                 url: str,
                                 credential_name: str,
                                 comment: Optional[str] = None,
                                 read_only: bool = False) -> Any:
        """
        Create an external location in Databricks.

        This function registers an external storage location (e.g., an S3 bucket)
        so that Databricks can query data stored in that location.

        Args:
            name (str): The unique name for the external location.
            url (str): The S3 (or other cloud) URL pointing to the external storage.
            credential_name (str): The name of the storage credential that provides access.
            comment (Optional[str]): An optional comment/description.
            read_only (bool): Whether the location should be marked as read-only.

        Returns:
            Any: An ExternalLocationInfo object representing the created external location.

        Raises:
            Exception: Propagates any exceptions raised during creation.
        """
        self.logger.debug(
            f"Creating external location: name={name}, url={url}, credential={credential_name}, "
            f"comment={comment}, read_only={read_only}")
        try:
            ext_loc = self.workspace_client.external_locations.create(
                name=name,
                url=url,
                credential_name=credential_name,
                comment=comment,
                read_only=read_only)
            self.logger.info(
                f"External location '{name}' created successfully: {ext_loc}")
            return ext_loc
        except Exception as e:
            self.logger.error(
                f"Error creating external location '{name}': {e}")
            raise

    def get_external_location(self, name: str) -> Any:
        """
        Retrieve details of an external location by name.

        Args:
            name (str): The name of the external location.

        Returns:
            Any: The ExternalLocationInfo object for the given external location.

        Raises:
            Exception: Propagates any exceptions raised during retrieval.
        """
        self.logger.debug(f"Retrieving external location: {name}")
        try:
            ext_loc = self.workspace_client.external_locations.get(name=name)
            self.logger.info(
                f"Retrieved external location '{name}': {ext_loc}")
            return ext_loc
        except Exception as e:
            self.logger.error(
                f"Error retrieving external location '{name}': {e}")
            raise

    def list_external_locations(self) -> List[Any]:
        """
        List all external locations registered in the current Databricks workspace.

        Returns:
            List[Any]: A list of ExternalLocationInfo objects.

        Raises:
            Exception: Propagates any exceptions raised during the listing.
        """
        self.logger.debug("Listing external locations.")
        try:
            locations = self.workspace_client.external_locations.list()
            self.logger.debug("Available External Locations:")
            for loc in locations:
                self.logger.debug(f" - {loc.name}")
            return locations
        except Exception as e:
            self.logger.error(f"Error listing external locations: {e}")
            raise

    def delete_external_location(self, name: str) -> None:
        """
        Delete an external location by name, with the force option enabled.

        Args:
            name (str): The name of the external location to delete.

        Raises:
            Exception: Propagates any exceptions raised during deletion.
        """
        self.logger.debug(f"Deleting external location: {name}")
        try:
            self.workspace_client.external_locations.delete(name=name,
                                                            force=True)
            self.logger.info(
                f"External location '{name}' deleted successfully.")
        except Exception as e:
            self.logger.error(
                f"Error deleting external location '{name}': {e}")
            raise

    # Catalog Management
    def create_catalog(self,
                       name: Optional[str] = None,
                       comment: Optional[str] = None,
                       storage_root: Optional[str] = None) -> Any:
        """
        Create a catalog for storing Iceberg table metadata.

        Args:
            name (Optional[str]): The catalog name. Defaults to the service's catalog_name.
            comment (Optional[str]): A comment for the catalog.
            storage_root (Optional[str]): The root storage location for the catalog data.

        Returns:
            Any: The created catalog object.

        Raises:
            Exception: Propagates any exceptions raised during creation.
        """
        catalog = name if name is not None else self.catalog_name
        self.logger.debug(
            f"Creating catalog: name={catalog}, comment={comment}, storage_root={storage_root}"
        )
        try:
            cat = self.workspace_client.catalogs.create(
                name=catalog, comment=comment, storage_root=storage_root)
            self.logger.info(
                f"Catalog '{catalog}' created successfully: {cat}")
            return cat
        except Exception as e:
            self.logger.error(f"Error creating catalog '{catalog}': {e}")
            raise

    def get_catalog(self, name: Optional[str] = None) -> Any:
        """
        Retrieve details of a catalog from Databricks.

        Args:
            name (Optional[str]): The catalog name. Defaults to the service's catalog_name.

        Returns:
            Any: The catalog object.

        Raises:
            Exception: Propagates any exceptions raised during retrieval.
        """
        catalog = name if name is not None else self.catalog_name
        self.logger.debug(f"Retrieving catalog: {catalog}")
        try:
            cat = self.workspace_client.catalogs.get(name=catalog)
            self.logger.info(f"Retrieved catalog '{catalog}': {cat}")
            return cat
        except Exception as e:
            self.logger.error(f"Error retrieving catalog '{catalog}': {e}")
            raise

    def list_catalogs(self) -> List[Any]:
        """
        List all catalogs in the current Databricks workspace.

        Returns:
            List[Any]: A list of catalog objects.

        Raises:
            Exception: Propagates any exceptions raised during the listing.
        """
        self.logger.debug("Listing catalogs.")
        try:
            cats = self.workspace_client.catalogs.list()
            self.logger.debug("Available Catalogs:")
            for cat in cats:
                self.logger.debug(f" - {cat.name}")
            return cats
        except Exception as e:
            self.logger.error(f"Error listing catalogs: {e}")
            raise

    def delete_catalog(self, name: Optional[str] = None) -> None:
        """
        Delete a catalog by name.

        Args:
            name (Optional[str]): The catalog name. Defaults to the service's catalog_name.

        Raises:
            Exception: Propagates any exceptions raised during deletion.
        """
        catalog = name if name is not None else self.catalog_name
        self.logger.debug(f"Deleting catalog: {catalog}")
        try:
            self.workspace_client.catalogs.delete(name=catalog)
            self.logger.info(f"Catalog '{catalog}' deleted successfully.")
        except Exception as e:
            self.logger.error(f"Error deleting catalog '{catalog}': {e}")
            raise

    # Tables Management
    def create_table(self, sql: str) -> None:
        """
        Create a table by executing the provided SQL DDL command.

        Args:
            sql (str): The SQL command to create the table.

        Raises:
            Exception: Propagates any exceptions raised during table creation.
        """
        self.logger.debug(f"Creating table with SQL:\n{sql}")
        spark = SparkSession.builder \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .getOrCreate()
        try:
            spark.sql(sql)
            self.logger.info("Table created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")
            raise

    def create_iceberg_table(self, schema: str, table: str, columns: dict,
                             table_location: str) -> None:
        """
        Create an Iceberg table in the specified schema using a dynamically generated SQL DDL command.

        Assumes that the active catalog is configured via the cluster.

        Args:
            schema (str): The schema (or database) within the active catalog.
            table (str): The name of the table to create.
            columns (dict): A mapping of column names to data types (e.g., {"id": "INT", "data": "STRING"}).
            table_location (str): The storage location for the table data.

        Raises:
            Exception: Propagates any exceptions raised during table creation.
        """
        self.logger.debug(
            f"Creating Iceberg table: {self.catalog_name}.{schema}.{table}")
        self.logger.debug(f"Table columns: {columns}")
        self.logger.debug(f"Table location: {table_location}")
        try:
            spark = SparkSession.builder \
                .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
                .getOrCreate()
            # Switch active catalog (assuming the cluster supports Unity Catalog)
            spark.sql(f"USE CATALOG {self.catalog_name};")
            self.logger.debug(f"Switched to catalog: {self.catalog_name}")
            # Build column definitions and final SQL (using two-part namespace: schema.table)
            columns_sql = ",\n  ".join(
                [f"{col} {dtype}" for col, dtype in columns.items()])
            full_table_name = f"{schema}.{table}"
            sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
              {columns_sql}
            )
            USING ICEBERG
            LOCATION '{table_location}'
            """
            self.logger.debug("Final SQL for creating Iceberg table:")
            self.logger.debug(sql)
            self.create_table(sql)
        except Exception as e:
            self.logger.error(
                f"Error creating Iceberg table '{self.catalog_name}.{schema}.{table}': {e}"
            )
            raise

    def get_table(self, sql: str) -> Any:
        """
        Retrieve table data by executing a SQL query.

        Args:
            sql (str): SQL query command.

        Returns:
            A Spark DataFrame with the query results.
        """
        spark = SparkSession.builder.getOrCreate()
        try:
            result = spark.sql(sql)
            self.logger.debug("Query executed successfully.")
            return result
        except Exception as e:
            self.logger.debug(f"Error retrieving table data: {e}")
            raise

    def delete_iceberg_table(self, schema: str, table: str) -> None:
        """
        Delete an Iceberg table in the active catalog by specifying its schema and table name.
        
        Args:
            schema (str): The schema (or database) name within the active catalog.
            table (str): The name of the table to delete.

        Raises:
            Exception: Propagates any exceptions raised during deletion.
        """
        fqtn = f"{self.catalog_name}.{schema}.{table}"
        self.logger.debug(f"Deleting table: {fqtn}")
        try:
            self.workspace_client.tables.delete(fqtn)
            self.logger.info(f"Table '{fqtn}' deleted successfully.")
        except Exception as e:
            self.logger.error(f"Error deleting table '{fqtn}': {e}")
            raise

    def delete_all_tables_in_schema(self, schema: str) -> None:
        """
        Delete all tables in the specified schema of the active catalog using the Databricks SDK.

        Args:
            schema (str): The schema (or database) name within the active catalog.

        Raises:
            Exception: Propagates any exceptions raised during listing or deletion.
        """
        self.logger.debug(
            f"Deleting all tables in schema: {self.catalog_name}.{schema}")
        try:
            tables = self.workspace_client.tables.list(self.catalog_name,
                                                       schema)
            tables_to_delete = []

            for table in tables:
                tables_to_delete.append(table)

            if not tables_to_delete:
                self.logger.debug(
                    f"No tables found in {self.catalog_name}.{schema}.")
            else:
                for table in tables_to_delete:
                    fqtn = table.name
                    try:
                        self.workspace_client.tables.delete(fqtn)
                        self.logger.info(f"Deleted table {fqtn}")
                    except Exception as e:
                        self.logger.error(f"Error deleting table {fqtn}: {e}")
                self.logger.debug("All tables in schema deleted successfully.")
        except Exception as e:
            self.logger.error(
                f"Error listing tables in {self.catalog_name}.{schema}: {e}")
            raise

    def delete_schema(self, schema: str) -> None:
        """
        Delete a schema from the active catalog using the Databricks SDK.

        Args:
            schema (str): The schema (or database) name to delete.

        Raises:
            Exception: Propagates any exceptions raised during deletion.
        """
        fqsn = f"{self.catalog_name}.{schema}"
        self.logger.debug(f"Deleting schema: {fqsn}")
        try:
            self.workspace_client.schemas.delete(fqsn)
            self.logger.info(f"Schema '{fqsn}' deleted successfully.")
        except Exception as e:
            self.logger.error(f"Error deleting schema '{fqsn}': {e}")
            raise

    def run_query(self, sql: str) -> Any:
        """
        Execute a SQL query and return the result as a Spark DataFrame.

        Args:
            sql (str): The SQL query to execute.

        Returns:
            Any: A Spark DataFrame containing the query results.

        Raises:
            Exception: Propagates any exceptions raised during query execution.
        """
        self.logger.debug(f"Running query:\n{sql}")
        spark = SparkSession.builder.getOrCreate()
        try:
            result = spark.sql(sql)
            self.logger.info("Query executed successfully.")
            return result
        except Exception as e:
            self.logger.error(f"Error running query: {e}")
            raise
