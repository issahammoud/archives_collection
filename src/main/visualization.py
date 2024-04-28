from src.utils.app import app
from src.helpers.layout import Layout
import src.utils.callbacks

app.layout = Layout.get_layout()
server = app.server


if __name__ == "__main__":
    app.run_server(debug=True, host="localhost", port=8000)
