from dash import dcc, html
import plotly.graph_objs as go
from dash_iconify import DashIconify
import dash_mantine_components as dmc

from src.helpers.db_connector import DBConnector


engine = DBConnector.get_engine(DBConnector.DBNAME)


class ImgLayout:
    @staticmethod
    def get_image(src, _id):
        fig = go.Figure()

        img_width, img_height = 780, 520
        scale_factor = 1

        fig.add_trace(
            go.Scatter(
                x=[0, img_width * scale_factor],
                y=[0, img_height * scale_factor],
                mode="markers",
                marker_opacity=0,
            )
        )

        fig.update_xaxes(
            visible=False,
            range=[0, img_width * scale_factor],
        )

        fig.update_yaxes(
            visible=False, range=[0, img_height * scale_factor], scaleanchor="x"
        )

        fig.add_layout_image(
            dict(
                x=0,
                sizex=img_width * scale_factor,
                y=img_height * scale_factor,
                sizey=img_height * scale_factor,
                xref="x",
                yref="y",
                opacity=1.0,
                layer="below",
                sizing="stretch",
                source=src,
            )
        )

        fig.update_layout(
            # width=img_width * scale_factor,
            height=img_height * scale_factor,
            margin={"l": 0, "r": 0, "t": 0, "b": 0, "pad": 0},
        )

        graph = dcc.Graph(
            id=_id,
            figure=fig,
            responsive=True,
            config={
                "staticPlot": False,
                "displaylogo": False,
                "modeBarButtonsToRemove": [
                    "select",
                    "zoomIn",
                    "zoomOut",
                    "autoScale",
                    "resetScale",
                    "select",
                ],
                "toImageButtonOptions": {
                    "format": "png",
                    "filename": "profile",
                    "height": 520,
                    "width": 780,
                    "scale": 1,
                },
            },
            style={
                "height": f"{img_height}px",
                "width": f"{img_width}px",
            },
        )

        style = {
            "box-shadow": "0 2px 4px rgba(0, 0, 0, 0.2)",
            "border-radius": "8px",
        }

        div = html.Div(graph, style=style)
        return div


class Layout:

    @staticmethod
    def get_main_section(src, title, content, tag):
        img = ImgLayout.get_image(src=src, _id="img")

        download = dmc.ActionIcon(
            DashIconify(icon="fluent:cloud-download-28-regular", width=28),
            size="sm",
            variant="subtle",
            id="download-btn",
            style={"position": "absolute", "top": 10, "right": 10, "zIndex": 1},
        )

        main = dmc.Grid(
            [
                download,
                dcc.Download(id="download-ctn"),
                dmc.Col(img, span=6),
                dmc.Col(
                    dmc.Stack(
                        [
                            dmc.Title(title, order=5),
                            dmc.Paper(content, shadow="md", p="md", radius="sm"),
                            dmc.Text(tag, size="sm"),
                        ]
                    ),
                    span=4,
                    offset=2,
                ),
            ],
            align="stretch",
            justify="flex-start",
            style={"position": "relative"},
        )

        return main

    @staticmethod
    def get_pagination(active_page, max):
        pagination = dmc.Grid(
            [
                dmc.Col(
                    dmc.Pagination(
                        id="pagination",
                        total=max,
                        withEdges=True,
                        withControls=True,
                        size="lg",
                        page=active_page,
                        siblings=1,
                    ),
                    span=8,
                ),
                dmc.Col(
                    dmc.Group(
                        [
                            dmc.Tooltip(
                                dmc.ActionIcon(
                                    DashIconify(icon="nonicons:go-16"),
                                    size="lg",
                                    variant="subtle",
                                    id="go_to_page",
                                ),
                                label="Go to",
                                offset=3,
                                withArrow=True,
                            ),
                            dmc.NumberInput(
                                value=active_page,
                                id="page_id",
                                min=1,
                                step=1,
                                max=max,
                                hideControls=True,
                                style={"width": 80},
                            ),
                        ],
                        spacing=0,
                        position="center",
                    ),
                    span=2,
                ),
            ],
            justify="center",
            align="stretch",
        )
        return pagination

    @staticmethod
    def get_layout():
        page = 1
        max_page = DBConnector.get_total_rows_count(engine, DBConnector.TABLE)
        pagination = Layout.get_pagination(page, max_page)

        return dmc.Container(
            [
                dmc.Container(
                    html.Div(pagination, id="left_side"),
                    size="xl",
                    style={"marginBottom": 30},
                ),
                dmc.Container(
                    html.Div(id="main"), size="xl", style={"marginBottom": 50}
                ),
            ],
            size="xl",
        )
