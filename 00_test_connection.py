"""
Basic connection test script. Run this first to verify Databricks access works.
"""
import base64
from io_utils.json import load_json
from rest.settings import parse_from_env_vars
from rest.driver.rest_api_db_storage import RestApiMetadataDBStorage
from cryptography.fernet import Fernet
from io_utils.secrets import get_aws_secrets

from urllib.parse import urlparse
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal






settings = parse_from_env_vars()
db = load_json(
    settings.rest_api_driver.file_storage_path + '/db.json.gz',
    RestApiMetadataDBStorage,
    compressed=True
)

connector = db.connectors_map['suffolk']

cipher = Fernet(get_aws_secrets('kumo-suffolk-dbx-secret-key').get('credentials_encryption_key', None))

CLIENT_ID = cipher.decrypt(
    base64.b64decode(
        connector.encrypted_credentials.client_id
    )
).decode('utf-8')


CLIENT_SECRET = cipher.decrypt(
    base64.b64decode(
        connector.encrypted_credentials.client_secret
    )
).decode('utf-8')



HTTP_PATH = f"/sql/1.0/warehouses/{connector.config.warehouse_id}"  # better: copy exact value from UI


def credential_provider():
    return oauth_service_principal(
        Config(
            host=connector.config.host,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
    )

query = """
SELECT *
FROM kumoai.models.budget
LIMIT 5
"""

with sql.connect(
    server_hostname=connector.config.host.split("//")[-1],
    http_path=HTTP_PATH,
    credentials_provider=credential_provider,
) as conn:
    with conn.cursor() as cur:
        cur.execute(query)
        print(cur.fetchall())
