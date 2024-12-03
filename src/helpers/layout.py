import base64
import pandas as pd
from dash import dcc, html
import plotly.express as px
from datetime import datetime
import plotly.graph_objs as go
from dash_iconify import DashIconify
import dash_mantine_components as dmc
from src.helpers.db_connector import DBConnector
from src.helpers.enum import Archives, DBCOLUMNS

engine = DBConnector.get_engine(DBConnector.DBNAME)


class Graph:
    def get_graph(df):
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
        )
        fig.update_layout(layout)
        fig.update_layout(yaxis_title=None)
        fig.update_layout(xaxis_title=None)
        fig.update_traces(marker_color="#0097b2")
        graph = dcc.Graph(
            figure=fig,
            style={"height": 60},
            config={"displaylogo": False, "displayModeBar": False},
        )
        return graph


class Layout:
    PAGES = 3

    @staticmethod
    def get_header():
        byte_img = open("src/helpers/logo.png", "rb").read()
        src = "data:image/png;base64,{}".format(
            base64.b64encode(byte_img).decode("ascii")
        )
        enroll_btn = Layout.get_enroll_btn()
        header = dmc.Grid(
            [
                dmc.GridCol(
                    html.Div(dmc.Image(fallbackSrc=src), style={"width": 120}), span=2
                ),
                dmc.GridCol(enroll_btn, span=1, offset=9),
            ],
            align="center",
            justify="flex-start",
        )
        return header

    @staticmethod
    def get_enroll_btn():
        link = "https://intuitive-dl.thinkific.com/courses/intuitive-dl"

        button = dmc.Button(
            dmc.Text("Enroll Now", c="#0097b2", fw=300),
            color="#ff5757",
            variant="outline",
        )
        tooltip = dmc.Tooltip(
            button,
            label="Enroll in Intuitive Deep Learning course for free",
            multiline=True,
            withArrow=True,
            openDelay=3,
            position="bottom",
        )
        a = html.A(
            children=tooltip,
            href=link,
            target="_blank",
            style={"textDecoration": "none"},
        )
        return a

    @staticmethod
    def filter_by_text():
        return dmc.Textarea(
            id="query",
            placeholder="Search by text",
            variant="subtle",
            autosize=True,
            leftSection=dmc.ActionIcon(
                DashIconify(icon="material-symbols-light:search"),
                variant="subtle",
                color="#0097b2",
                id="filter_by_text",
            ),
        )

    @staticmethod
    def filter_by_archive():
        return dmc.Grid(
            dmc.GridCol(
                dmc.MultiSelect(
                    placeholder="Select an archive",
                    checkIconPosition="right",
                    hidePickedOptions=True,
                    maxDropdownHeight=200,
                    searchable=True,
                    clearable=True,
                    leftSectionPointerEvents="none",
                    leftSection=DashIconify(icon="bi-book", color="#0097b2"),
                    variant="subtle",
                    data=[val.title() for val in Archives],
                    comboboxProps={
                        "transitionProps": {"transition": "pop", "duration": 200}
                    },
                ),
                span=12,
            ),
            align="center",
            justify="center",
        )

    @staticmethod
    def filter_by_date():
        date = dmc.DatePickerInput(
            id="date",
            valueFormat="DD/MM/YYYY",
            allowSingleDateInRange=False,
            value=DBConnector.get_min_max_dates(engine, DBConnector.TABLE),
            type="range",
            leftSection=DashIconify(icon="uiw:date", color="#0097b2"),
            clearable=True,
            variant="subtle",
        )
        return date

    @staticmethod
    def filter_by_tag():
        tags = DBConnector.get_tags(engine, DBConnector.TABLE)
        tags = ["All"] + [tag.title() for tag in tags if tag]

        select = dmc.Select(
            id="tag",
            data=tags,
            placeholder="Select a topic",
            checkIconPosition="right",
            maxDropdownHeight=200,
            searchable=True,
            variant="subtle",
            rightSectionPointerEvents="none",
            rightSection=None,
            comboboxProps={"transitionProps": {"transition": "pop", "duration": 200}},
            leftSection=DashIconify(icon="icon-park-outline:topic", color="#0097b2"),
        )
        return select

    @staticmethod
    def get_navbar():
        date = Layout.filter_by_date()
        tag = Layout.filter_by_tag()
        text = Layout.filter_by_text()
        archives = Layout.filter_by_archive()
        return dmc.Paper(
            dmc.Stack(
                [date, tag, text, archives],
                align="stretch",
                justify="space-between",
                gap="xl",
            ),
            p=20,
            shadow="xs",
            h="100%",
        )

    @staticmethod
    def get_stats(df):
        stats_bar = dmc.Grid(
            [
                dmc.GridCol(
                    Graph.get_graph(df),
                    span=12,
                ),
            ],
            justify="center",
            align="flex-end",
        )
        return stats_bar

    @staticmethod
    def get_card(byte_img, title, content, tag, archive, date, link):
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
                            dmc.Text(archive, fw=600),
                            dmc.Text(date, fs="italic", fw=300, ta="right"),
                        ],
                        grow=True,
                        justify="space-around",
                    ),
                    withBorder=True,
                    inheritPadding=True,
                    py="sm",
                ),
                dmc.CardSection(
                    dmc.Image(fallbackSrc=src, radius=2, h=300),
                    withBorder=True,
                    mb=10,
                ),
                dmc.CardSection(
                    [
                        dmc.Stack(
                            [
                                dmc.Tooltip(
                                    dmc.Text(
                                        title,
                                        fw=500,
                                        size="md",
                                        truncate=True,
                                        lineClamp=1,
                                        ta="left",
                                    ),
                                    label=title,
                                    multiline=True,
                                    withArrow=True,
                                    openDelay=3,
                                    position="top-start",
                                ),
                                dmc.Blockquote(
                                    dmc.ScrollArea(
                                        dmc.Text(content, size="sm", ta="justify"),
                                        type="hover",
                                        offsetScrollbars=True,
                                        scrollbarSize=4,
                                        h=120,
                                    ),
                                    color="#0097b2",
                                    radius=6,
                                    iconSize=40,
                                    p="md",
                                    h=150,
                                    icon=DashIconify(
                                        icon="material-symbols:format-quote-rounded",
                                        width=20,
                                        color="#ff5757",
                                        style={
                                            "position": "absolute",
                                            "top": 20,
                                            "left": 20,
                                            "zIndex": 1,
                                            "transform": "scaleY(-1) scaleX(-1)",
                                        },
                                    ),
                                ),
                                dmc.Box(
                                    dmc.Text(
                                        tag,
                                        size="sm",
                                        c="dimmed",
                                        fs="italic",
                                        ta="left",
                                    ),
                                    p=5,
                                ),
                            ]
                        )
                    ],
                    withBorder=True,
                    inheritPadding=True,
                ),
            ],
            withBorder=True,
            shadow="xs",
            radius="md",
        )
        return html.A(
            children=card, href=link, target="_blank", style={"textDecoration": "none"}
        )

    @staticmethod
    def get_carousel(args):
        carousel = dmc.Carousel(
            [
                dmc.CarouselSlide(Layout.get_card(*args[i]), style={"width": "33.33%"})
                for i in range(len(args))
            ],
            id="carousel",
            slideSize="33.33%",
            w="100%",
            slideGap="md",
            loop=False,
            align="start",
            slidesToScroll=Layout.PAGES,
            nextControlIcon=DashIconify(
                icon="material-symbols:navigate-next", color="#0097b2"
            ),
            previousControlIcon=DashIconify(
                icon="material-symbols:navigate-before", color="#0097b2"
            ),
        )
        return carousel

    @staticmethod
    def get_footer():
        return dmc.Box(
            dmc.Text(
                "© Copyright Intuitive Deep Learning 2024. All rights reserved.",
                size="sm",
            ),
            mt=8,
            p=24,
        )

    @staticmethod
    def get_layout():
        header = Layout.get_header()
        navbar = Layout.get_navbar()
        df = pd.DataFrame(DBConnector.group_by_month(engine, DBConnector.TABLE))
        stats = Layout.get_stats(df)

        args = DBConnector.fetch_data_keyset(
            engine,
            DBConnector.TABLE,
            DBCOLUMNS.rowid,
            limit=100,
            direction="desc",
            filters={
                DBCOLUMNS.image: ("notnull", None),
                DBCOLUMNS.date: ("gt", datetime(2000, 1, 1)),
            },
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
        carousel = Layout.get_carousel(args)

        main = dmc.Stack([stats, carousel], gap="xl", align="stretch")

        footer = Layout.get_footer()

        appshell = dmc.AppShell(
            [
                dmc.AppShellHeader(
                    dmc.Container(
                        header,
                        size="xl",
                    ),
                    withBorder=True,
                ),
                dmc.AppShellNavbar(
                    navbar,
                    withBorder=False,
                ),
                dmc.AppShellMain(dmc.Container(main, id="main", size="xl", mt=30)),
                dmc.AppShellFooter(footer, withBorder=False),
            ],
            header={"height": 120},
            navbar={
                "width": 280,
                "breakpoint": "sm",
                "collapsed": {"mobile": True},
            },
            footer={"height": 60},
        )
        return dmc.MantineProvider(appshell)
