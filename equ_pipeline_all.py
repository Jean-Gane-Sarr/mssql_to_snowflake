import dagster as dagster
import dlt
from dlt.common import pendulum
from dlt.sources import incremental
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database
import urllib.parse
import os


# To return a subset of columns
def table_adapter_callback(table):
    if table.name == 'V_facture_dashboard_am':
        columns_to_keep = [
            "IDVLinkLocalisation",
            "CodeConstructeurSource",
            "NumeroSerieSource",
            "DateHeureCreation",
            "DateHeureDonnee",
            "Latitude",
            "Longitude",
            "validity",
            "serviceMeter_value",
            "serviceMeter_uom",
            "ModuleCode",
            "ModuleTime",
            "receivedTime",
            "IDdMessage",
            "IDMasterMessage",
            "SourceDonnee"
        ]
        for col in list(table._columns):
            if col.name not in columns_to_keep:
                table._columns.remove(col)
    return table


def get_mssql_engine():

    driver   = os.getenv("MSSQL_DRIVER")
    server   = os.getenv("MSSQL_SERVER")
    port     = os.getenv("MSSQL_PORT")
    database = os.getenv("MSSQL_DATABASE")
    username = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")

    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "MARS_Connection=yes;"   # allows parallel queries
        "Packet Size=32767;"     # larger network packets
        "Connection Timeout=60;"  # Increase timeout
        "Command Timeout=300;"    # Increase command timeout
    )
    conn_str_encoded = urllib.parse.quote_plus(conn_str)

    engine_url = f"mssql+pyodbc:///?odbc_connect={conn_str_encoded}&arraysize=50000"
    return engine_url


##### EXTRACT V_facture_dashboard_am #############

@dlt.resource(
    name="V_facture_dashboard_am",
    ##columns={
    ##    "Mesure": {"data_type": "decimal", "precision": 18, "scale": 4},
    ##    "TargetValue": {"data_type": "decimal", "precision": 18, "scale": 4},
    ##},
    write_disposition="replace",
    #primary_key="IDVLinkLocalisation",
    #parallelized=True
)
def get_facture_data():
    
    source_sta = sql_database(
        get_mssql_engine(),
        backend="pyarrow",
        chunk_size=300_000,
        reflection_level="minimal",
        include_views=True,
       # table_adapter_callback=table_adapter_callback,
    ).with_resources("V_facture_dashboard_am").parallelize()
    
     
    # Activate incremental loading
    ##source_sta.V_facture_dashboard_am.apply_hints(incremental=incremental("IDVLinkLocalisation"))

    return source_sta


@dlt.source
def facture_source():
    return get_facture_data()

if __name__ == "__main__":
    
    pipeline = dlt.pipeline(
        pipeline_name = "facture_dashboard_am_pipeline",
        destination = "snowflake",
        dataset_name = "equipement",
        progress="log",
    )
    
    load_info = pipeline.run(
        facture_source(),
    )
    
    print("\n" + "#"*200 + "\n")
    #print(pipeline.dataset().a_bronze_vlinklocalisation.df())
    print("\n" + "#"*200 + "\n")
