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
    def get_header():
        byte_img = open("src/helpers/logo.png", "rb").read()
        src = "data:image/png;base64,{}".format(
            base64.b64encode(byte_img).decode("ascii")
        )
        header = dmc.Grid(
            [
                dmc.GridCol(
                    html.Div(dmc.Image(fallbackSrc=src), style={"width": 120}), span=2
                )
            ],
            align="center",
            justify="flex-start",
        )
        return header

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
        tags = ["All"] + [tag[0].title() for tag in tags if tag[0]]

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
        return dmc.Box(
            dmc.Stack(
                [date, tag, text, archives],
                align="stretch",
                justify="space-between",
                gap="xl",
            ),
            mt=8,
            p=20,
        )

    @staticmethod
    def get_stats(df):
        stats_bar = dmc.Grid(
            [
                dmc.GridCol(
                    Graph.get_graph(
                        df,
                        DBConnector.get_min_max_dates(engine, DBConnector.TABLE),
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
                    dmc.Image(
                        fallbackSrc=src,
                        h=300,
                    ),
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
                            dmc.ScrollArea(
                                dmc.Text(
                                    content,
                                    size="sm",
                                    style={
                                        "verticalAlign": "top",
                                        "textAlign": "justify",
                                    },
                                ),
                                type="hover",
                                offsetScrollbars=True,
                                h=150,
                                scrollbarSize=4,
                            ),
                            color="#0097b2",
                            radius="sm",
                            icon=DashIconify(
                                icon="teenyicons:quote-solid",
                                width=25,
                                color="#ff5757",
                                style={
                                    "position": "absolute",
                                    "top": 25,
                                    "left": 30,
                                    "zIndex": 1,
                                },
                            ),
                            style={
                                "height": 150,
                                "background": "white",
                            },
                        ),
                        dmc.CardSection(
                            dmc.Text(
                                tag, size="sm", c="dimmed", fs="italic", ta="left"
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
            style={"height": "100%", "width": "100%"},
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

        return html.Div(carousel, style={"width": "100%"})

    @staticmethod
    def get_affix():
        link = "https://intuitive-dl.thinkific.com/courses/intuitive-dl"

        button = dmc.Button("Enroll Now!", color="#ff5757")
        tooltip = dmc.Tooltip(
            button,
            label="Enroll in Intuitive Deep Learning course",
            multiline=True,
            withArrow=True,
            openDelay=3,
        )
        a = html.A(
            children=tooltip,
            href=link,
            target="_blank",
            style={"textDecoration": "none"},
        )
        affix = dmc.Affix(a, position={"bottom": 20, "right": 20})
        return affix

    @staticmethod
    def get_footer():
        return dmc.Box(
            dmc.Text("© Copyright Intuitive Deep Learning 2024", size="sm"), mt=8, p=24
        )

    @staticmethod
    def get_layout():
        header = Layout.get_header()
        navbar = Layout.get_navbar()
        df = pd.DataFrame(DBConnector.group_by_month(engine, DBConnector.TABLE))
        stats = Layout.get_stats(df)

        args = DBConnector.get_first_n_rows(
            engine,
            DBConnector.TABLE,
            100,
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
        carousel = Layout.get_carousel(args)

        main = dmc.Container(dmc.Stack([stats, carousel], gap="xl"), size="xl")

        affix = Layout.get_affix()
        footer = Layout.get_footer()

        appshell = dmc.AppShell(
            [
                affix,
                dmc.AppShellHeader(
                    dmc.Container(
                        header,
                        size="xl",
                    ),
                    withBorder=True,
                ),
                dmc.AppShellNavbar(
                    dmc.Container(navbar, size="xl"),
                    withBorder=True,
                ),
                dmc.AppShellMain(dmc.Container(main, id="main", size="xl")),
                dmc.AppShellFooter(dmc.Container(footer, size="xl"), withBorder=False),
            ],
            header={"height": 120},
            padding="xl",
            navbar={
                "width": 300,
                "breakpoint": "sm",
                "collapsed": {"mobile": True},
            },
        )
        return dmc.MantineProvider(appshell)
