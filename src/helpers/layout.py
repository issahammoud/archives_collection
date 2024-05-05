import base64
import numpy as np
from dash import dcc, html
import plotly.express as px
from datetime import datetime
import plotly.graph_objs as go
from dash_iconify import DashIconify
import dash_mantine_components as dmc
from src.helpers.db_connector import DBConnector


engine = DBConnector.get_engine(DBConnector.DBNAME)


class Graph:
    def get_graph(df, range_date):
        layout = go.Layout(
            xaxis=dict(
                autorange=True,
                showgrid=True,
                # ticks='',
                showticklabels=True,
                visible=True,
            ),
            yaxis=dict(autorange=True, visible=False),
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        fig = px.bar(
            df,
            x="month",
            y="count",
            hover_data={"month": "|%B %d, %Y"},
            range_x=range_date,
        )
        fig.update_layout(layout)
        fig.update_layout(yaxis_title=None)
        fig.update_layout(xaxis_title=None)
        graph = dcc.Graph(
            figure=fig,
            style={"height": 50},
            config={"displaylogo": False, "displayModeBar": False},
        )
        return graph


class Layout:
    @staticmethod
    def get_navbar():
        tags = DBConnector.get_tags(engine, DBConnector.TABLE)
        tags = ["All"] + [tag[0].title() for tag in tags if tag[0]]
        header = dmc.Header(
            dmc.Grid(
                [
                    dmc.Col(
                        dmc.Group(
                            [
                                dmc.TextInput(
                                    id="query",
                                    placeholder="Filter by text",
                                    rightSection=DashIconify(
                                        icon="material-symbols-light:search"
                                    ),
                                ),
                                dmc.Button(
                                    "Search",
                                    id="filter_by_text",
                                    size="sm",
                                    variant="light",
                                ),
                            ],
                            align="center",
                            spacing="xs",
                        ),
                        span=4,
                    ),
                    dmc.Col(
                        dmc.Select(id="tag", data=tags, value="All", searchable=True),
                        span=2,
                        offset=4,
                    ),
                    dmc.Col(
                        dmc.DateRangePicker(
                            id="date",
                            allowSingleDateInRange=False,
                            clearable=True,
                            value=DBConnector.get_min_max_dates(
                                engine, DBConnector.TABLE
                            ),
                        ),
                        span=2,
                    ),
                ],
            ),
            height=100,
            withBorder=True,
            style={
                "backgroundColor": "#5ca6ee",
                "padding": "0 10px 0 10px",
                "boxShadow": "0 2px 1px rgba(207, 218, 228, .2)",
                "borderRadius": "4px",
            },
        )

        return header

    @staticmethod
    def get_pagination(active_page, max, df):
        pagination = dmc.Grid(
            [
                dmc.Col(
                    dmc.Pagination(
                        id="pagination",
                        total=int(np.round(max / 3)),
                        withEdges=True,
                        withControls=True,
                        size="lg",
                        page=active_page,
                        siblings=0,
                    ),
                    span=6,
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
                                style={"width": 50},
                            ),
                        ],
                        spacing=0,
                        position="center",
                    ),
                    span=1,
                ),
                dmc.Col(
                    Graph.get_graph(
                        df,
                        DBConnector.get_min_max_dates(engine, DBConnector.TABLE_VIEW),
                    ),
                    span="auto",
                ),
            ],
            justify="center",
            align="flex-end",
        )
        return pagination

    @staticmethod
    def get_card(byte_img, title, content, tag, archive, date):
        src = "data:image/png;base64,{}".format(base64.b64encode(byte_img).decode())
        date = datetime.strftime(date, "%B, %d %Y")
        archive = archive.strip().title() if archive else archive
        tag = tag.strip().title() if tag else tag
        content = content.strip() if content else content
        card = dmc.Card(
            children=[
                dmc.CardSection(
                    dmc.Group(
                        [
                            dmc.Text(archive, fw=700),
                            dmc.Text(date, italic=True, fw=400, align="center"),
                        ],
                        grow=True,
                    ),
                    withBorder=True,
                    inheritPadding=True,
                    py="xs",
                ),
                dmc.CardSection(
                    dmc.Image(src=src, height=300),
                    mb=10,
                ),
                dmc.Text(title, fw=500, size="md", truncate=True),
                dmc.Blockquote(
                    dmc.Text(content, size="sm", align="justify"),
                    color="red",
                    style={"height": 150, "overflowY": "hidden"},
                ),
                dmc.CardSection(
                    dmc.Text(tag, size="sm", color="dimmed", italic=True), pb=10, pl=20
                ),
            ],
            withBorder=True,
            shadow="sm",
            radius="md",
            style={"height": "100%"},
        )
        return card

    @staticmethod
    def get_main_section(args):
        cards = []
        for i in range(len(args)):
            cards.append(Layout.get_card(*args[i]))

        main = dmc.Grid(
            [dmc.Col(card, span=12 // len(args)) for card in cards],
            align="stretch",
            justify="center",
        )

        return main

    @staticmethod
    def get_layout():
        header = Layout.get_navbar()

        return dmc.Container(
            [
                dmc.Container(
                    header,
                    size="xl",
                    style={"marginBottom": 50},
                ),
                dmc.Container(
                    html.Div(id="left_side"),
                    size="xl",
                    style={"marginBottom": 30},
                ),
                dmc.Container(
                    html.Div(id="main"), size="xl", style={"marginBottom": 50}
                ),
            ],
            size="xl",
        )
