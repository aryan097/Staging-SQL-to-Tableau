# main.py

import logging
from sql_connect import fetch_pivot_ready_inline
from tableau_publish import publish_dataframe_to_tableau

# -------- Feature toggles --------
RUN_SQL_AGG_AND_FETCH = True
RUN_TABLEAU_PUBLISH   = False  # turn True when ready to publish

# -------- SQL params --------
SQL_CONFIG_PATH   = "sql.json"
PRODUCT_LABEL     = "Product Not Appropriate"
START_DATE        = None       # e.g., "2024-12-01"
END_DATE          = None       # e.g., "2024-12-31"
WITH_GRAND_TOTAL  = True

# -------- Tableau params --------
TABLEAU_SERVER_URL = "https://your-tableau-server"
TABLEAU_SITE_NAME  = ""            # Default site
TABLEAU_PROJECT    = "Default"
TABLEAU_DS_NAME    = "PA_Pivot"
TABLEAU_PAT_NAME   = "your-pat-name"
TABLEAU_PAT_TOKEN  = "your-pat-token"
TABLEAU_VERIFY_SSL = True

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger("main")
    pivot_df = None

    if RUN_SQL_AGG_AND_FETCH:
        pivot_df = fetch_pivot_ready_inline(
            cfg_path=SQL_CONFIG_PATH,
            product_label=PRODUCT_LABEL,
            start_date=START_DATE,
            end_date=END_DATE,
            with_grand_total=WITH_GRAND_TOTAL,
        )

    if RUN_TABLEAU_PUBLISH:
        if pivot_df is None or pivot_df.empty:
            logger.error("No pivot_df available to publish. Enable RUN_SQL_AGG_AND_FETCH or supply a DataFrame.")
            raise SystemExit(1)

        ds_id = publish_dataframe_to_tableau(
            df=pivot_df,
            datasource_name=TABLEAU_DS_NAME,
            server_url=TABLEAU_SERVER_URL,
            pat_name=TABLEAU_PAT_NAME,
            pat_token=TABLEAU_PAT_TOKEN,
            project_name=TABLEAU_PROJECT,
            site_name=TABLEAU_SITE_NAME,
            verify_ssl=TABLEAU_VERIFY_SSL,
        )
        logger.info("Publish complete. Datasource ID: %s", ds_id)
