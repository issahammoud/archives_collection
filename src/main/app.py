import os
import dash
import dash_mantine_components as dmc
from src.helpers.layout import Layout
import src.utils.callbacks


dash._dash_renderer._set_react_version("18.2.0")
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=dmc.styles.ALL,
    assets_folder=os.path.join(os.getcwd(), "assets"),
)

app.layout = Layout.get_layout()
server = app.server


if __name__ == "__main__":

    app.run_server(debug=True, host="0.0.0.0", port=8050)
