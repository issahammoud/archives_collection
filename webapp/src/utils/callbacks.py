import os
import dash
import logging
import pandas as pd
from dash.exceptions import PreventUpdate
from dash_extensions.enrich import Input, State, Output, callback

from src.utils.utils import get_query_embedding
from src.helpers.db_connector import DBConnector
from src.helpers.layout import Layout, Navbar, Main
from src.helpers.enum import DBCOLUMNS, OPERATORS, CeleryTasks, JobsKeys
from src.utils.celery_tasks import collection_task, revoke_task, download_task


logger = logging.getLogger(__name__)
engine = DBConnector.get_engine()


def get_filters_dict(archive, tag, date_range, submit, null_clicks, query):
    filters = {DBCOLUMNS.date: [(OPERATORS.ge, date_range[0])]} if date_range[0] else {}
    if date_range[1]:
        filters[DBCOLUMNS.date].append((OPERATORS.le, date_range[1]))

    filters.update({DBCOLUMNS.tag: [(OPERATORS.like, tag.strip())]} if tag else {})

    filters.update({DBCOLUMNS.archive: [(OPERATORS.in_, archive)]} if archive else {})

    if submit and query:
        if len(query.split()) == 1:
            filters.update({DBCOLUMNS.text_searchable: [(OPERATORS.ts, query)]})
        else:
            embedding = get_query_embedding(query, os.getenv("EMBED_URL"))
            if embedding:
                filters.update({DBCOLUMNS.embedding: [(OPERATORS.vs, embedding)]})
            else:
                logger.debug("Falling back to tsvector search")
                filters.update({DBCOLUMNS.text_searchable: [(OPERATORS.ts, query)]})

    filters.update(
        {DBCOLUMNS.image: [(OPERATORS.notnull, None)]}
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
            DBConnector.group_by(engine, DBConnector.TABLE, groupby, filters),
            columns=["date", "count"],
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
                args = args if direction == "forward" else args[::-1]

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
    Output("navbar_col", "span"),
    Output("main", "span"),
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
        navbar_span = 2 if not opened else 0
        main_span = 12 - navbar_span
        return not opened, style, navbar_span, main_span
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
            DBConnector.group_by(engine, DBConnector.TABLE, value, filters),
            columns=["date", "count"],
        )
        return Main.get_stats(df, not order), False
    raise PreventUpdate


@callback(
    Output("download_status", "data"),
    Output("download_interval", "disabled"),
    Output("notification-container", "sendNotifications"),
    Input("download", "n_clicks"),
    State("states", "data"),
    prevent_initial_call=True,
)
def trigger_download(n_clicks, states):
    if n_clicks:
        archive = states["archive"]
        tag = states["tag"]
        date_range = states["date_range"]
        query = states["query"]
        null_clicks = states["null_clicks"]
        order = states["order"]

        filters = get_filters_dict(archive, tag, date_range, True, null_clicks, query)

        columns = [
            DBCOLUMNS.rowid,
            DBCOLUMNS.date,
            DBCOLUMNS.archive,
            DBCOLUMNS.link,
            DBCOLUMNS.title,
            DBCOLUMNS.content,
            DBCOLUMNS.tag,
            DBCOLUMNS.image,
        ]
        try:
            task = download_task.apply_async(args=(columns, filters, order))
            job_status = {
                JobsKeys.TASKID: task.id,
                JobsKeys.STATUS: "start",
                JobsKeys.TASKNAME: CeleryTasks.download,
            }
            return job_status, False, Layout.download_notif()
        except Exception as e:
            logger.error(f"Failed to start task: {str(e)}")
            return dash.no_update, True, dash.no_update

    raise PreventUpdate


@callback(
    Output("redirect", "pathname"),
    Output("download_interval", "disabled", allow_duplicate=True),
    Input("download_interval", "n_intervals"),
    State("download_status", "data"),
    prevent_initial_call=True,
)
def get_downloaded_data(n, job_status):
    if (
        job_status
        and JobsKeys.TASKID in job_status
        and job_status[JobsKeys.TASKNAME] == CeleryTasks.download
    ):
        task_id = job_status[JobsKeys.TASKID]
        result = download_task.AsyncResult(task_id)
        if result.state == "PENDING" or result.state == "STARTED":
            return dash.no_update, False
        elif result.state == "FAILURE":
            return dash.no_update, True
        else:
            return "/download-data", True
    raise PreventUpdate


@callback(
    Output("job_status", "data", allow_duplicate=True),
    Output("interval", "disabled"),
    Output("notification-container", "sendNotifications", allow_duplicate=True),
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
            job_status = {
                JobsKeys.TASKID: task.id,
                JobsKeys.STATUS: "start",
                JobsKeys.TASKNAME: CeleryTasks.collect,
            }
            return job_status, False, Layout.collect_notif()
        except Exception as e:
            logger.error(f"Failed to start task: {str(e)}")
            return dash.no_update, True, dash.no_update
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
        task_id = status[JobsKeys.TASKID]
        revoke_task(task_id)
        status[JobsKeys.STATUS] = "STOP"
        return status
    raise PreventUpdate


@callback(
    Output("interval", "disabled", allow_duplicate=True),
    Output("start_collect", "disabled", allow_duplicate=True),
    Output("stop_collect", "disabled", allow_duplicate=True),
    Input("job_status", "data"),
    prevent_initial_call=False,
)
def sync_controls_on_load(job_status):
    if (
        not job_status
        or JobsKeys.TASKID not in job_status
        or job_status[JobsKeys.TASKNAME] == CeleryTasks.download
    ):
        return True, False, True

    task_id = job_status[JobsKeys.TASKID]
    desired = job_status.get(JobsKeys.STATUS, "")
    async_result = collection_task.AsyncResult(task_id)

    if desired == "STOP" or async_result.state in ("REVOKED", "SUCCESS", "FAILURE"):
        return True, False, True

    return False, True, False
