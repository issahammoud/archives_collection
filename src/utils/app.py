import dash
import dash_mantine_components as dmc


dash._dash_renderer._set_react_version("18.2.0")
app = dash.Dash(
    __name__, suppress_callback_exceptions=True, external_stylesheets=dmc.styles.ALL
)
