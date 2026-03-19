"""
Shared Databricks connection setup for Suffolk data extraction.
Run in the argo environment where kumo imports and AWS secrets are available.
"""
import base64
from io_utils.json import load_json
from rest.settings import parse_from_env_vars
from rest.driver.rest_api_db_storage import RestApiMetadataDBStorage
from cryptography.fernet import Fernet
from io_utils.secrets import get_aws_secrets
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal


def get_connection():
    settings = parse_from_env_vars()
    db = load_json(
        settings.rest_api_driver.file_storage_path + '/db.json.gz',
        RestApiMetadataDBStorage,
        compressed=True,
    )

    connector = db.connectors_map['suffolk']
    cipher = Fernet(
        get_aws_secrets('kumo-suffolk-dbx-secret-key').get('credentials_encryption_key')
    )

    client_id = cipher.decrypt(
        base64.b64decode(connector.encrypted_credentials.client_id)
    ).decode('utf-8')

    client_secret = cipher.decrypt(
        base64.b64decode(connector.encrypted_credentials.client_secret)
    ).decode('utf-8')

    host = connector.config.host
    http_path = f"/sql/1.0/warehouses/{connector.config.warehouse_id}"

    def credential_provider():
        return oauth_service_principal(
            Config(host=host, client_id=client_id, client_secret=client_secret)
        )

    conn = sql.connect(
        server_hostname=host.split("//")[-1],
        http_path=http_path,
        credentials_provider=credential_provider,
    )
    return conn


# Table name mapping: alias -> full Databricks table path
TABLES = {
    "models_project": "kumoai.models.project",
    "models_timecard": "kumoai.models.timecard",
    "models_construction_log": "kumoai.models.construction_log",
    "models_observation": "kumoai.models.observation",
    "models_task_update": "kumoai.models.task_update",
    "kumo_transformations_distinct_projects": "kumoai.kumo_transformations.distinct_projects",
    "kumo_transformations_gr_weekly": "kumoai.kumo_transformations.gr_weekly",
    "models_schedule_update": "kumoai.models.schedule_update",
    "models_schedule_baseline": "kumoai.models.schedule_baseline",
    "models_incident": "kumoai.models.incident",
}
