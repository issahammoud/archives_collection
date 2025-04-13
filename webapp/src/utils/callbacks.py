import copy
import dash
import logging
import pandas as pd
from dash.exceptions import PreventUpdate
from dash_extensions.enrich import Input, State, Output, callback, dcc
from src.helpers.enum import DBCOLUMNS
from src.utils.utils import prepare_query
from src.helpers.db_connector import DBConnector
from src.helpers.layout import Layout, Navbar, Main
from src.utils.celery_tasks import collection_task, revoke_task


logger = logging.getLogger(__name__)
engine = DBConnector.get_engine()


def get_filters_dict(archive, tag, date_range, submit, null_clicks, query):
    filters = {DBCOLUMNS.date: [("ge", date_range[0])]} if date_range[0] else {}
    if date_range[1]:
        filters[DBCOLUMNS.date].append(("le", date_range[1]))

    filters.update({DBCOLUMNS.tag: [("like", tag.strip())]} if tag else {})

    filters.update({DBCOLUMNS.archive: [("in", archive)]} if archive else {})

    filters.update(
        {
            DBCOLUMNS.text_searchable: [("text_search", prepare_query(query))],
        }
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
    Output("previous_active", "data"),
    Output("states", "data"),
    Input("archive", "value"),
    Input("tag", "value"),
    Input("date", "value"),
    Input("query", "n_submit"),
    Input("asc_desc", "n_clicks"),
    Input("null_img", "n_clicks"),
    Input("interval", "n_intervals"),
    State("query", "value"),
    State("groupby", "value"),
)
def create_content(
    archive, tag, date_range, submit, sort_clicks, null_clicks, n, query, groupby
):
    filters = get_filters_dict(archive, tag, date_range, submit, null_clicks, query)

    order = sort_clicks is None or not sort_clicks % 2
    states = {
        "archive": archive,
        "tag": tag,
        "date_range": date_range,
        "submit": submit,
        "null_clicks": null_clicks,
        "order": order,
        "query": query,
        "groupby": groupby,
    }

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

    args = args if args else []
    total_count = DBConnector.get_total_count(engine, DBConnector.TABLE, filters)
    total_count = total_count if total_count else 0

    badge = Navbar.get_badge(total_count)
    if len(args) > Layout.SLIDES:
        df = pd.DataFrame(
            DBConnector.group_by(engine, DBConnector.TABLE, groupby, filters)
        )
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

        return Main.get_main(df, args, not order), badge, last_seen, 0, states
    return Main.get_alert(), badge, dash.no_update, 0, states


@callback(
    Output("carousel", "children"),
    Output("carousel", "initialSlide"),
    Output("last_seen", "data", allow_duplicate=True),
    Output("previous_active", "data", allow_duplicate=True),
    Input("carousel", "active"),
    State("previous_active", "data"),
    State("last_seen", "data"),
    State("states", "data"),
    prevent_initial_call=True,
)
def update_carousel(active, previous_active, last_seen, states):
    if active is not None:
        archive = states["archive"]
        tag = states["tag"]
        date_range = states["date_range"]
        query = states["query"]
        order = states["order"]
        null_clicks = states["null_clicks"]

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
                desc_order=order,
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
                Main.get_carousel_slides(args),
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
            style.update({"left": 0, "transform": "rotate(0deg)"})
            if opened
            else style.update(
                {"left": "calc(15% - 15px)", "transform": "rotate(180deg)"}
            )
        )
        return not opened, style
    raise PreventUpdate


@callback(
    Output("stats_bar", "children"),
    Output("popover", "opened"),
    Input("groupby", "value"),
    State("states", "data"),
    prevent_initial_call=True,
)
def group_by(value, states):
    if value:
        archive = states["archive"]
        tag = states["tag"]
        date_range = states["date_range"]
        query = states["query"]
        order = states["order"]
        null_clicks = states["null_clicks"]

        filters = get_filters_dict(archive, tag, date_range, True, null_clicks, query)

        df = pd.DataFrame(
            DBConnector.group_by(engine, DBConnector.TABLE, value, filters)
        )
        return Main.get_stats(df, not order), False
    raise PreventUpdate


@callback(
    Output("download_csv", "data"),
    Input("download", "n_clicks"),
    State("states", "data"),
    prevent_initial_call=True,
)
def download(n_clicks, states):
    if n_clicks:
        archive = states["archive"]
        tag = states["tag"]
        date_range = states["date_range"]
        query = states["query"]
        null_clicks = states["null_clicks"]

        filters = get_filters_dict(archive, tag, date_range, True, null_clicks, query)
        columns = [col for col in list(DBCOLUMNS) if "text_searchable" not in col]
        data = DBConnector.get_all_rows(engine, DBConnector.TABLE, filters, columns)
        df = pd.DataFrame(data, columns=columns)
        return dcc.send_data_frame(df.to_csv, "data.csv")

    raise PreventUpdate


@callback(
    Output("job_status", "data"),
    Output("interval", "disabled"),
    Input("start_collect", "n_clicks"),
    State("states", "data"),
    prevent_initial_call=True,
)
def start_collection(n_clicks, states):
    if n_clicks:
        date_range = states.get("date_range")
        archive = states.get("archive")

        try:
            task = collection_task.apply_async(
                args=(archive, date_range[0], date_range[1])
            )
            return {"task_id": task.id, "status": "start"}, False
        except Exception as e:
            print(f"Failed to start task: {str(e)}")
            return dash.no_update, True
    raise PreventUpdate


@callback(
    Output("job_status", "data", allow_duplicate=True),
    Input("stop_collect", "n_clicks"),
    State("job_status", "data"),
    prevent_initial_call=True,
)
def stop_collection(n_clicks, job_status):
    if n_clicks:
        status = job_status.copy()
        status["status"] = "stop"
        return status
    raise PreventUpdate


@callback(
    Output("interval", "disabled", allow_duplicate=True),
    Output("start_collect", "disabled"),
    Output("stop_collect", "disabled"),
    Input("interval", "n_intervals"),
    State("job_status", "data"),
    prevent_initial_call=True,
)
def refresh(n, job_status):
    if job_status and "task_id" in job_status:
        task_id = job_status["task_id"]
        status = job_status["status"]
        task = collection_task.AsyncResult(task_id)

        if status == "stop":
            revoke_task(task_id)
            return True, False, True
        if task.state == "SUCCESS":
            return True, False, True
        return False, True, False
    raise PreventUpdate
