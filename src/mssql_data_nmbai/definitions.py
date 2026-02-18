from pathlib import Path

from dagster import Definitions, load_from_defs_folder, ScheduleDefinition, define_asset_job
from dagster_embedded_elt.dlt import DagsterDltResource
from mssql_data_nmbai.defs.assets import(
    equipment_dashboard_assets, 
    devis_dashboard_assets, 
    commande_dashboard_assets,
    facture_dashboard_assets
)


#jobs
equipment_dashboard_job = define_asset_job(
    name="equipment_dashboard_job",
    selection=[equipment_dashboard_assets],
)
devis_dashboard_job = define_asset_job(
    name="devis_dashboard_job",
    selection=[devis_dashboard_assets],
)
commande_dashboard_job = define_asset_job(
    name="commande_dashboard_job",
    selection=[commande_dashboard_assets],
)
facture_dashboard_job = define_asset_job(
    name="facture_dashboard_job",
    selection=[facture_dashboard_assets],
)

#schedule : every day
equipment_dashboard_schedule = ScheduleDefinition(
    job=equipment_dashboard_job,
    cron_schedule="0 0 * * *", ## every day
)

devis_dashboard_schedule = ScheduleDefinition(
    job= devis_dashboard_job,
    cron_schedule="0 0 * * *", ## every day
)

commande_dashboard_schedule = ScheduleDefinition(
    job= commande_dashboard_job,
    cron_schedule="0 0 * * *", ## every day
)

facture_dashboard_schedule = ScheduleDefinition(
    job= facture_dashboard_job,
    cron_schedule="0 0 * * *", ## every day
)


defs = Definitions(
    jobs= [equipment_dashboard_job,devis_dashboard_job,commande_dashboard_job,facture_dashboard_job],
    assets=[equipment_dashboard_assets,devis_dashboard_assets,commande_dashboard_assets,facture_dashboard_assets],
    resources={
        "dlt":DagsterDltResource(),
    },
    #selection=[equipment_dashboard_assets,devis_dashboard_assets,commande_dashboard_assets,facture_dashboard_assets],
    schedules = [equipment_dashboard_schedule,devis_dashboard_schedule,commande_dashboard_schedule,facture_dashboard_schedule]
)


