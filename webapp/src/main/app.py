import os
import dash
from flask import send_file
from dash import CeleryManager
import dash_mantine_components as dmc
from dash_extensions.enrich import DashProxy, ServersideOutputTransform, RedisBackend
from src.utils.logging import logging
from src.main.celery_app import celery_app

from src.helpers.layout import Layout
import src.utils.callbacks


logger = logging.getLogger(__name__)
dash._dash_renderer._set_react_version("18.2.0")


background_callback_manager = CeleryManager(celery_app)


app = DashProxy(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=dmc.styles.ALL,
    assets_folder=os.path.join(os.getcwd(), "assets"),
    background_callback_manager=background_callback_manager,
    transforms=[
        ServersideOutputTransform(backends=[RedisBackend(host=os.environ["REDIS_URL"])])
    ],
)

app.layout = Layout.get_layout
server = app.server


@server.route("/download-data")
def stream_data_zip():
    zip_path = "/images/data.zip"
    if not os.path.exists(zip_path):
        return "File not found", 404

    return send_file(
        zip_path,
        mimetype="application/zip",
        as_attachment=True,
        download_name="data.zip",
    )


if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=8050)
