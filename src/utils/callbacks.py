import pandas as pd
from dash.exceptions import PreventUpdate
from dash import Input, State, Output, callback, html


from src.helpers.layout import Layout
from src.helpers.enum import DBCOLUMNS
from src.helpers.db_connector import DBConnector


engine = DBConnector.get_engine(DBConnector.DBNAME)


@callback(
    Output("left_side", "children"),
    [
        Input("tag", "value"),
        Input("date", "value"),
        Input("filter_by_text", "n_clicks"),
        Input("switch", "checked"),
    ],
    State("query", "value"),
)
def create_content(tag, date_range, n_clicks, switch, query):
    if tag or date_range or n_clicks:
        min_max_date = DBConnector.get_min_max_dates(engine, DBConnector.TABLE)
        if tag != "All" or min_max_date != date_range or n_clicks or switch:
            DBConnector.create_view(
                engine,
                DBConnector.TABLE,
                DBConnector.VIEW,
                tag,
                date_range,
                query,
                switch,
            )
            DBConnector.TABLE_VIEW = DBConnector.VIEW
        else:
            DBConnector.TABLE_VIEW = DBConnector.TABLE

        page = 1
        df = pd.DataFrame(DBConnector.group_by_month(engine, DBConnector.TABLE_VIEW))
        max_page = DBConnector.get_total_rows_count(engine, DBConnector.TABLE_VIEW)
        pagination = Layout.get_pagination(page, max_page, df)
        return pagination
    return html.Div(id="pagination")


@callback(
    Output("main", "children"),
    [Input("pagination", "page"), Input("left_side", "children")],
)
def change_page(page, _):
    if page:
        page = min(
            page,
            int(
                round(
                    DBConnector.get_total_rows_count(engine, DBConnector.TABLE_VIEW) / 3
                )
            ),
        )
        page_border = (3 * page - 2, 3 * page)
        args = DBConnector.get_n_rows(
            engine,
            DBConnector.TABLE_VIEW,
            page_border,
            columns=[
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
            return Layout.get_main_section(args)
    return html.Div()


@callback(
    Output("pagination", "page"),
    Input("go_to_page", "n_clicks"),
    State("page_id", "value"),
)
def go_to_page(clicks, page):
    if clicks:
        return page
    raise PreventUpdate
