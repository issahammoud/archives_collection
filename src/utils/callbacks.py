import dash
import pandas as pd
from dash.exceptions import PreventUpdate
from dash import Input, State, Output, callback


from src.helpers.layout import Layout
from src.helpers.enum import DBCOLUMNS
from src.helpers.db_connector import DBConnector


engine = DBConnector.get_engine(DBConnector.DBNAME)


def get_filters_dict(archive, tag, date_range, submit, null_clicks, query):
    filters = {DBCOLUMNS.date: [("ge", date_range[0])]}
    if date_range[1]:
        filters[DBCOLUMNS.date].append(("le", date_range[1]))

    filters.update({DBCOLUMNS.tag: [("like", tag.strip())]} if tag else {})

    filters.update({DBCOLUMNS.archive: [("in", archive)]} if archive else {})

    filters.update(
        {DBCOLUMNS.text_searchable: [("text_search", query)]}
        if submit and query
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
    Output("badge", "children"),
    Output("last_seen", "data"),
    Input("archive", "value"),
    Input("tag", "value"),
    Input("date", "value"),
    Input("query", "n_submit"),
    Input("asc_desc", "n_clicks"),
    Input("null_img", "n_clicks"),
    State("query", "value"),
)
def create_content(archive, tag, date_range, submit, sort_clicks, null_clicks, query):
    filters = get_filters_dict(archive, tag, date_range, submit, null_clicks, query)

    df = pd.DataFrame(DBConnector.group_by_day(engine, DBConnector.TABLE, filters))
    order = sort_clicks is None or not sort_clicks % 2
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
        desc_order=order,
    )
    total_count = DBConnector.get_total_count(engine, DBConnector.TABLE, filters)
    badge = Layout.get_badge(total_count)
    if len(args) > Layout.SLIDES:
        last_seen = {
            "forward": {
                DBCOLUMNS.date: args[-1 - Layout.SLIDES][-2],
                DBCOLUMNS.rowid: args[-1 - Layout.SLIDES][0],
            },
            "backward": {
                DBCOLUMNS.date: args[Layout.SLIDES][-2],
                DBCOLUMNS.rowid: args[Layout.SLIDES][0],
            },
        }

        return Layout.get_main(df, args, not order), badge, last_seen
    return Layout.get_alert(), badge, dash.no_update


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
    query,
    sort_clicks,
    null_clicks,
):
    if active is not None:
        filters = get_filters_dict(archive, tag, date_range, True, null_clicks, query)

        direction = "forward" if active > previous_active else "backward"

        if (
            (direction == "forward" and active == Layout.MAX_PAGES - 1)
            or (direction == "backward" and active == 0)
        ) and previous_active != 0:

            args = DBConnector.fetch_data_keyset(
                engine,
                DBConnector.TABLE,
                limit=Layout.SLIDES * Layout.MAX_PAGES,
                filters=filters,
                last_seen_value=last_seen[direction],
                direction=direction,
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
                desc_order=(sort_clicks is None or not sort_clicks % 2),
            )
            if len(args) > Layout.SLIDES:
                last_seen["forward"] = {
                    DBCOLUMNS.date: args[-1 - Layout.SLIDES][-2],
                    DBCOLUMNS.rowid: args[-1 - Layout.SLIDES][0],
                }
                last_seen["backward"] = {
                    DBCOLUMNS.date: args[Layout.SLIDES][-2],
                    DBCOLUMNS.rowid: args[Layout.SLIDES][0],
                }
            return (
                Layout.get_carousel_slides(args),
                0 if direction == "forward" else Layout.MAX_PAGES - 1,
                last_seen,
                0,
            )

        return dash.no_update, active, last_seen, active
    raise PreventUpdate


@callback(
    Output("drawer", "opened"),
    Output("open_drawer", "style"),
    Input("open_drawer", "n_clicks"),
    State("drawer", "opened"),
    State("open_drawer", "style"),
)
def open_close_drawer(n_clicks, opened, style):
    if n_clicks:
        (
            style.update({"left": 0})
            if opened
            else style.update({"left": "calc(12% - 15px)"})
        )
        return not opened, style
    raise PreventUpdate
