import base64
from dash import dcc, html
import plotly.express as px
from datetime import datetime
import plotly.graph_objs as go
from dash_iconify import DashIconify
import dash_mantine_components as dmc
from src.helpers.db_connector import DBConnector
from src.helpers.enum import Archives


engine = DBConnector.get_engine(DBConnector.DBNAME)


class Graph:
    def get_graph(df, range_date):
        layout = go.Layout(
            xaxis=dict(
                autorange=True,
                showgrid=True,
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
    PAGES = 3

    @staticmethod
    def get_navbar():
        tags = DBConnector.get_tags(engine, DBConnector.TABLE)
        tags = ["All"] + [tag[0].title() for tag in tags if tag[0]]
        header = dmc.Grid(
            [
                dmc.GridCol(
                    dmc.Menu(
                        [
                            dmc.MenuTarget(
                                dmc.ActionIcon(
                                    DashIconify(icon="material-symbols:menu"),
                                    size="lg",
                                    variant="filled",
                                    color="blue",
                                )
                            ),
                            dmc.MenuDropdown(
                                [
                                    dmc.MenuLabel("Filters"),
                                    dmc.MenuItem(
                                        dmc.Select(
                                            id="tag",
                                            data=tags,
                                            value="All",
                                            w=180,
                                            maxDropdownHeight=200,
                                            searchable=True,
                                        ),
                                        leftSection=DashIconify(
                                            icon="icon-park-outline:topic"
                                        ),
                                        closeMenuOnClick=False,
                                    ),
                                    dmc.MenuItem(
                                        dmc.DatePicker(
                                            id="date",
                                            valueFormat="DD/MM/YY",
                                            allowSingleDateInRange=False,
                                            value=DBConnector.get_min_max_dates(
                                                engine, DBConnector.TABLE
                                            ),
                                            type="multiple",
                                            w=180,
                                        ),
                                        leftSection=DashIconify(icon="uiw:date"),
                                        closeMenuOnClick=False,
                                    ),
                                    dmc.MenuItem(
                                        dmc.Switch(
                                            id="switch",
                                            size="sm",
                                            offLabel=DashIconify(
                                                icon="iconoir:off-tag", width=20
                                            ),
                                            onLabel=DashIconify(
                                                icon="iconoir:on-tag", width=20
                                            ),
                                        ),
                                        closeMenuOnClick=False,
                                    ),
                                    dmc.MenuDivider(),
                                    dmc.MenuLabel("Search"),
                                    dmc.MenuItem(
                                        dmc.TextInput(
                                            id="query",
                                            placeholder="Search by text",
                                            w=180,
                                        ),
                                        leftSection=dmc.ActionIcon(
                                            DashIconify(
                                                icon="material-symbols-light:search"
                                            ),
                                            id="filter_by_text",
                                        ),
                                        closeMenuOnClick=False,
                                    ),
                                    dmc.MenuItem(
                                        dmc.TextInput(
                                            id="article_id",
                                            placeholder="Search by URL",
                                            w=180,
                                        ),
                                        leftSection=dmc.ActionIcon(
                                            DashIconify(icon="mynaui:hash"),
                                            id="filter_by_hash",
                                        ),
                                        closeMenuOnClick=False,
                                    ),
                                ]
                            ),
                        ],
                        trigger="hover",
                        closeOnItemClick=False,
                        closeOnEscape=False,
                        withArrow=True,
                    ),
                    span=1,
                ),
            ],
            align="center",
            justify="flex-end",
        )

        return header

    @staticmethod
    def archive_filter():
        ckeckbox_group = dmc.CheckboxGroup(
            [
                dmc.Group(
                    [dmc.Checkbox(label=val.title(), value=val) for val in Archives],
                    align="center",
                ),
            ],
            id="source",
            value=[val.value for val in Archives],
        )
        toggle = dmc.Switch(
            id="toggle",
            checked=True,
            onLabel=DashIconify(
                icon="teenyicons:tick-outline", width=20, color="green"
            ),
            offLabel=DashIconify(icon="akar-icons:cross", width=20, color="red"),
        )
        return dmc.Center(
            dmc.Group([toggle, ckeckbox_group], align="flex-end", gap="xl")
        )

    @staticmethod
    def get_stats(df):
        stats_bar = dmc.Grid(
            [
                dmc.GridCol(
                    Graph.get_graph(
                        df,
                        DBConnector.get_min_max_dates(engine, DBConnector.TABLE_VIEW),
                    ),
                    span=12,
                ),
            ],
            justify="center",
            align="flex-end",
        )
        return stats_bar

    @staticmethod
    def get_card(rowid, byte_img, title, content, tag, archive, date, link):
        src = (
            "data:image/png;base64,{}".format(base64.b64encode(byte_img).decode())
            if byte_img
            else "https://placehold.co/600x400?text=Placeholder"
        )
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
                            dmc.Text(date, td="italic", fw=400),
                        ],
                        grow=True,
                    ),
                    withBorder=True,
                    inheritPadding=True,
                    py="xs",
                ),
                dmc.CardSection(
                    dmc.Image(fallbackSrc=src, h=300),
                    mb=10,
                ),
                dmc.Stack(
                    [
                        dmc.Tooltip(
                            dmc.Text(title, fw=500, size="md", truncate=True),
                            label=title,
                            multiline=True,
                            withArrow=True,
                            openDelay=3,
                        ),
                        dmc.Blockquote(
                            dmc.Text(
                                content,
                                size="sm",
                                style={"verticalAlign": "top", "textAlign": "justify"},
                            ),
                            color="blue",
                            radius="sm",
                            icon=DashIconify(
                                icon="teenyicons:quote-solid",
                                width=25,
                                color="red",
                                style={
                                    "position": "absolute",
                                    "top": 25,
                                    "left": 30,
                                    "zIndex": 1,
                                },
                            ),
                            style={
                                "height": 150,
                                "overflowY": "hidden",
                                "background": "white",
                            },
                        ),
                        dmc.CardSection(
                            dmc.Text(
                                tag, size="sm", c="dimmed", td="italic", ta="left"
                            ),
                            pb=10,
                            pl=20,
                        ),
                    ]
                ),
            ],
            withBorder=True,
            shadow="sm",
            radius="md",
            style={"height": "100%"},
        )
        return html.A(
            children=card, href=link, target="_blank", style={"textDecoration": "none"}
        )

    @staticmethod
    def get_main_section(args):
        cards = []
        previous = dmc.ActionIcon(
            DashIconify(icon="fluent:previous-32-filled"),
            id="previous",
            variant="subtle",
            size="xl",
            style={
                "position": "absolute",
                "zIndex": 100,
                "top": "50%",
                "left": 0,
                "marginLeft": "-50px",
            },
        )
        next = dmc.ActionIcon(
            DashIconify(icon="fluent:next-32-filled"),
            id="next",
            variant="subtle",
            size="xl",
            style={
                "position": "absolute",
                "zIndex": 100,
                "top": "50%",
                "right": 0,
                "marginRight": "-50px",
            },
        )

        for i in range(len(args)):
            card = Layout.get_card(*args[i])
            cards.append(dmc.GridCol(card, span=12 // len(args)))

        cards.extend([previous, next])
        main = dmc.Grid(cards, style={"position": "relative"})

        return main

    @staticmethod
    def get_layout():
        header = Layout.get_navbar()
        checkboxes = Layout.archive_filter()
        appshell = dmc.AppShell(
            [
                dmc.AppShellHeader(
                    dmc.Container(
                        header,
                        size="xl",
                    ),
                    withBorder=True,
                    p=10,
                ),
                dmc.Space(h=90),
                dmc.AppShellSection(
                    dmc.Container(
                        checkboxes,
                        size="xl",
                    ),
                    m=30,
                ),
                dmc.AppShellSection(
                    dmc.Container(
                        id="stats_bar",
                        size="xl",
                    ),
                    m=30,
                ),
                dmc.AppShellMain(dmc.Container(id="main", size="xl")),
            ]
        )
        return dmc.MantineProvider(appshell)
