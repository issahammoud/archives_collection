import pandas as pd
from dash.exceptions import PreventUpdate
from dash import Input, State, Output, callback, html


from src.helpers.layout import Layout
from src.helpers.enum import DBCOLUMNS, Archives
from src.helpers.db_connector import DBConnector


engine = DBConnector.get_engine(DBConnector.DBNAME)


@callback(
    Output("stats_bar", "children"),
    Output("main", "children"),
    Output("article_id", "value"),
    [
        Input("source", "value"),
        Input("tag", "value"),
        Input("date", "value"),
        Input("filter_by_text", "n_clicks"),
        Input("switch", "checked"),
    ],
    State("query", "value"),
)
def create_content(source, tag, date_range, n_clicks, switch, query):
    if source or tag or date_range or n_clicks:
        if len(source) == 0:
            return html.Div(id="pagination")

        min_max_date = DBConnector.get_min_max_dates(engine, DBConnector.TABLE)
        if source or tag != "All" or min_max_date != date_range or n_clicks or switch:
            DBConnector.create_view(
                engine,
                DBConnector.TABLE,
                DBConnector.VIEW,
                tag,
                date_range,
                query,
                switch,
                source,
            )
            DBConnector.TABLE_VIEW = DBConnector.VIEW
        else:
            DBConnector.TABLE_VIEW = DBConnector.TABLE

        df = pd.DataFrame(DBConnector.group_by_month(engine, DBConnector.TABLE_VIEW))
        stats_bar = Layout.get_stats(df)
        args = DBConnector.get_first_n_rows(
            engine,
            DBConnector.TABLE_VIEW,
            n=Layout.PAGES,
            columns=[
                DBCOLUMNS.rowid,
                DBCOLUMNS.image,
                DBCOLUMNS.title,
                DBCOLUMNS.content,
                DBCOLUMNS.tag,
                DBCOLUMNS.archive,
                DBCOLUMNS.date,
                DBCOLUMNS.link,
            ],
        )
        last_id = args[-1][0]
        main_section = Layout.get_main_section(args)
        return stats_bar, main_section, last_id
    raise PreventUpdate


@callback(
    Output("main", "children", allow_duplicate=True),
    Output("article_id", "value", allow_duplicate=True),
    Input("next", "n_clicks"),
    State("article_id", "value"),
    prevent_initial_call=True,
)
def next_page(clicks, id):
    if clicks:
        n = Layout.PAGES

        args = DBConnector.get_next_n_rows(
            engine,
            DBConnector.TABLE_VIEW,
            id,
            n,
            columns=[
                DBCOLUMNS.rowid,
                DBCOLUMNS.image,
                DBCOLUMNS.title,
                DBCOLUMNS.content,
                DBCOLUMNS.tag,
                DBCOLUMNS.archive,
                DBCOLUMNS.date,
                DBCOLUMNS.link,
            ],
        )
        if args:
            last_id = args[-1][0]
            return Layout.get_main_section(args), last_id
    raise PreventUpdate


@callback(Output("source", "value"), Input("toggle", "checked"))
def toggle_checkboxes(toggle):
    if toggle is not None:
        return [val.value for val in Archives] if toggle else []
    raise PreventUpdate
