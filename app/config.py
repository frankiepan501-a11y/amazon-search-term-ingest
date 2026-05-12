import os

LINGXING_APP_ID = os.environ.get("LINGXING_APP_ID", "ak_B1P0qz2mkImfS")
LINGXING_APP_SECRET = os.environ.get("LINGXING_APP_SECRET", "IMJm0f/dwDM7YYR+2FrlEQ==")

FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID", "cli_a9f6ae86fce8dbd8")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "r0eQTiBoP1WnQCUnBanMQeu5ACT57at7")
FEISHU_ALERT_USER_OPENID = os.environ.get("FEISHU_ALERT_USER_OPENID", "ou_629ce01f4bc31de078e10fcb038dbf78")
FEISHU_ALERT_GROUP_CHATID = os.environ.get("FEISHU_ALERT_GROUP_CHATID", "")

PG_HOST = os.environ.get("POSTGRESQL_HOST", "service-69856f0d2e156a6efa59a9cf")
PG_PORT = int(os.environ.get("POSTGRESQL_PORT", "5432"))
PG_USER = os.environ.get("POSTGRESQL_USER", "root")
PG_PASSWORD = os.environ.get("POSTGRESQL_PASSWORD", "0dRL5sQqkcw6KC74oif2BgeEu3IvT981")
PG_DATABASE = os.environ.get("POSTGRESQL_DATABASE", "zeabur")

API_BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN", "")

DSN = f"host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASSWORD} dbname={PG_DATABASE}"
