# tableau_publish.py
# Requires: pantab, tableauhyperapi, tableauserverclient

import logging
import tempfile
from pathlib import Path

import pantab
import pandas as pd
import tableauserverclient as TSC

LOGGER = logging.getLogger(__name__)

def publish_dataframe_to_tableau(
    df: pd.DataFrame,
    datasource_name: str,
    server_url: str,
    pat_name: str,
    pat_token: str,
    project_name: str = "Default",
    site_name: str = "",
    verify_ssl: bool = True,
) -> str:
    tmp_dir = tempfile.mkdtemp()
    hyper_path = str(Path(tmp_dir) / f"{datasource_name}.hyper")

    LOGGER.info("Writing DataFrame to Hyper: %s", hyper_path)
    pantab.frame_to_hyper(df, hyper_path, table="Extract")  # standard table name

    LOGGER.info("Signing in to Tableau Server: %s (site='%s')", server_url, site_name or "Default")
    tableau_auth = TSC.PersonalAccessTokenAuth(pat_name, pat_token, site_name)
    server = TSC.Server(server_url)
    if not verify_ssl:
        server.add_http_options({"verify": False})
    server.use_server_version()

    with server.auth.sign_in(tableau_auth):
        proj_id = None
        for p in TSC.Pager(server.projects):
            if p.name == project_name:
                proj_id = p.id
                break
        if proj_id is None:
            raise RuntimeError(f"Project '{project_name}' not found on server.")

        LOGGER.info("Publishing datasource '%s' to project '%s' ...", datasource_name, project_name)
        ds_item = TSC.DatasourceItem(project_id=proj_id, name=datasource_name)
        published = server.datasources.publish(
            ds_item, hyper_path, mode=TSC.Server.PublishMode.Overwrite
        )
        LOGGER.info("Published datasource id: %s", published.id)

    return str(published.id)
