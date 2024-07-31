import logging
import os
import json
import yaml
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    root_logger = logging.getLogger()
    log_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.setLevel(logging.INFO)  # DEBUG
    root_logger.addHandler(console_handler)

    # DEFINE BQ CLIENT
    project_id = os.environ["GCP_PROJECT_ID"]
    credentials = service_account.Credentials.from_service_account_info(json.loads(os.environ["GCP_SA"]))
    bq_client = bigquery.Client(project=project_id, credentials=credentials)

    with open("web.yaml", "r") as f:
        config = yaml.safe_load(f)

    with open("params.yaml", "r") as f:
        params = yaml.safe_load(f)

    # DATASET
    dataset_id = params['SOURCE_DATASET']
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound as nf:
        logger.info(nf)
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "EU"
            bq_client.create_dataset(dataset)
        except Conflict as c:
            logger.info(c)

    table_ref = dataset_ref.table(params['TABLE_ID'])
    table_schema = [
        bigquery.SchemaField(field['name'], field['type'], field['mode'])
        for field in config['schema']
    ]

    job_config = bigquery.LoadJobConfig(
        source_format='CSV',
        schema=table_schema,
        skip_leading_rows=1,
        write_disposition = 'WRITE_TRUNCATE' 
    )

    with open("web.csv", "rb") as source_file:
        job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()  # Waits for the job to complete.

    logger.info("Initial load to BigQuery: OK")
    
    with open("dataprep.sql", "r") as f:
        proc = f.read()

    job = bq_client.query(proc.format(PROJECT_ID = project_id,
                                      USER = os.environ["GCP_USER"], **params))
    job.result()
    logger.info("Data preparation: DONE")
