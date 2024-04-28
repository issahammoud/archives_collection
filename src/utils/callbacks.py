import io
import base64
import zipfile
import tempfile
from dash.exceptions import PreventUpdate
from dash import Input, State, Output, callback, dcc


from src.helpers.layout import Layout
from src.helpers.enum import DBCOLUMNS
from src.helpers.db_connector import DBConnector


engine = DBConnector.get_engine(DBConnector.DBNAME)


@callback(
    Output("main", "children"),
    Input("pagination", "page"),
)
def change_page(page):
    if page:
        args = DBConnector.get_row(
            engine,
            DBConnector.TABLE,
            page,
            columns=[
                DBCOLUMNS.image,
                DBCOLUMNS.title,
                DBCOLUMNS.content,
                DBCOLUMNS.tag,
            ],
        )
        max_page = DBConnector.get_total_rows_count(engine, DBConnector.TABLE)

        while args is None and page < max_page:
            page += 1
            args = DBConnector.get_row(
                engine,
                DBConnector.TABLE,
                page,
                columns=[
                    DBCOLUMNS.image,
                    DBCOLUMNS.title,
                    DBCOLUMNS.content,
                    DBCOLUMNS.tag,
                ],
            )

        img, title, content, tag = args

        src = base64.b64encode(img)
        src = "data:image/png;base64,{}".format(src.decode())
        return Layout.get_main_section(src, title, content, tag)
    raise PreventUpdate


@callback(
    [
        Output("main", "children", allow_duplicate=True),
        Output("pagination", "page"),
    ],
    Input("go_to_page", "n_clicks"),
    State("page_id", "value"),
    prevent_initial_call=True,
)
def go_to_page(clicks, page):
    if clicks:
        return change_page(page), page
    raise PreventUpdate


@callback(
    Output("download-ctn", "data"),
    Input("download-btn", "n_clicks"),
    [State("pagination", "page")],
    prevent_initial_call=True,
)
def download(n_clicks, page):
    if n_clicks and page:
        img, title, content, tag = DBConnector.get_row(
            engine,
            DBConnector.TABLE,
            page,
            columns=[
                DBCOLUMNS.image,
                DBCOLUMNS.title,
                DBCOLUMNS.content,
                DBCOLUMNS.tag,
            ],
        )
        text = "\n".join([title, content, tag])

        list_of_tuples = [("image.png", io.BytesIO(img)), ("text.txt", text)]

        with tempfile.NamedTemporaryFile(prefix="profile_", suffix=".zip") as tmp:
            with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, False) as zip_file:
                for file_name, data in list_of_tuples:
                    if isinstance(data, str):
                        zip_file.writestr(file_name, data)
                    else:
                        zip_file.writestr(file_name, data.read())

            return dcc.send_file(tmp.name)
    raise PreventUpdate
