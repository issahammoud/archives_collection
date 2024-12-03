import dash
import pandas as pd
from dash.exceptions import PreventUpdate
from dash import Input, State, Output, callback


from src.helpers.layout import Layout
from src.helpers.enum import DBCOLUMNS
from src.helpers.db_connector import DBConnector


engine = DBConnector.get_engine(DBConnector.DBNAME)


def get_filters_dict(archive, tag, date_range, text_clicks, null_clicks, query):
    filters = {DBCOLUMNS.date: [("ge", date_range[0]), ("le", date_range[1])]}

    filters.update({DBCOLUMNS.tag: [("eq", tag)]} if tag else {})

    filters.update({DBCOLUMNS.archive: [("in", archive)]} if archive else {})

    filters.update(
        {DBCOLUMNS.text_searchable: [("text_search", query)]}
        if text_clicks and query
        else {}
    )

    filters.update(
        {DBCOLUMNS.image: [("notnull", None)]}
        if null_clicks is not None and null_clicks % 2
        else {}
    )

    return filters


@callback(
    Output("main", "children"),
    Output("last_seen", "data"),
    Input("archive", "value"),
    Input("tag", "value"),
    Input("date", "value"),
    Input("text", "n_clicks"),
    Input("asc_desc", "n_clicks"),
    Input("null_img", "n_clicks"),
    State("query", "value"),
)
def create_content(
    archive, tag, date_range, text_clicks, sort_clicks, null_clicks, query
):
    filters = get_filters_dict(
        archive, tag, date_range, text_clicks, null_clicks, query
    )

    df = pd.DataFrame(DBConnector.group_by_day(engine, DBConnector.TABLE, filters))
    args = DBConnector.fetch_data_keyset(
        engine,
        DBConnector.TABLE,
        limit=Layout.SLIDES * Layout.MAX_PAGES,
        filters=filters,
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
        flip_order=not (sort_clicks is not None and sort_clicks % 2),
    )
    data = {
        DBCOLUMNS.date: args[-1 - Layout.SLIDES][-2],
        DBCOLUMNS.rowid: args[-1 - Layout.SLIDES][0],
        "direction": "forward",
    }

    return Layout.get_main(df, args), data


@callback(
    Output("carousel", "children"),
    Output("carousel", "initialSlide"),
    Output("last_seen", "data", allow_duplicate=True),
    Output("previous_active", "data"),
    Input("carousel", "active"),
    State("previous_active", "data"),
    State("last_seen", "data"),
    State("archive", "value"),
    State("tag", "value"),
    State("date", "value"),
    State("text", "n_clicks"),
    State("query", "value"),
    State("asc_desc", "n_clicks"),
    State("null_img", "n_clicks"),
    prevent_initial_call=True,
)
def update_carousel(
    active,
    previous_active,
    last_seen,
    archive,
    tag,
    date_range,
    text_clicks,
    query,
    sort_clicks,
    null_clicks,
):

    if active is not None:
        filters = get_filters_dict(
            archive, tag, date_range, text_clicks, null_clicks, query
        )

        direction = "forward" if active > previous_active else "backward"
        last_seen["direction"] = direction

        if direction == "forward" and active == Layout.MAX_PAGES - 1:

            args = DBConnector.fetch_data_keyset(
                engine,
                DBConnector.TABLE,
                limit=Layout.SLIDES * Layout.MAX_PAGES,
                filters=filters,
                last_seen_value=last_seen,
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
                flip_order=not (sort_clicks is not None and sort_clicks % 2),
            )
            last_seen[DBCOLUMNS.date] = args[-1 - Layout.SLIDES][-2]
            last_seen[DBCOLUMNS.rowid] = args[-1 - Layout.SLIDES][0]
            return Layout.get_carousel_slides(args), 0, last_seen, 0

        elif direction == "backward" and active == 0 and previous_active != 0:
            args = DBConnector.fetch_data_keyset(
                engine,
                DBConnector.TABLE,
                limit=Layout.SLIDES * Layout.MAX_PAGES,
                filters=filters,
                last_seen_value=last_seen,
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
                flip_order=sort_clicks % 2,
            )
            last_seen[DBCOLUMNS.date] = args[0][-2]
            last_seen[DBCOLUMNS.rowid] = args[0][0]
            return Layout.get_carousel_slides(args), Layout.MAX_PAGES - 2, last_seen, 0
        else:
            return dash.no_update, active, last_seen, active
    raise PreventUpdate
