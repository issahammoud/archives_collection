import base64
from dash import dcc, html
import plotly.express as px
from datetime import datetime
import plotly.graph_objs as go
from dash_iconify import DashIconify
import dash_mantine_components as dmc
from src.helpers.db_connector import DBConnector
from src.helpers.enum import Archives, DBCOLUMNS
from src.utils.utils import resize_image_for_html


engine = DBConnector.get_engine(DBConnector.DBNAME)


class Graph:
    def get_graph(df, order):
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
            x="day",
            y="count",
            hover_data={"day": "|%B %d, %Y"},
        )

        fig.update_layout(layout)
        fig.update_layout(
            xaxis=dict(title=None, autorange=order), yaxis=dict(title=None)
        )
        fig.update_traces(marker_color="#0097b2")
        graph = dcc.Graph(
            figure=fig,
            style={"height": 60},
            config={"displaylogo": False, "displayModeBar": False, "scrollZoom": True},
        )
        return graph


class Layout:
    SLIDES = 3
    MAX_PAGES = 10

    @staticmethod
    def get_header():
        byte_img = open("assets/logo.png", "rb").read()
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
            label="Enroll for free in Intuitive Deep Learning Course",
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
        return dmc.TextInput(
            id="query",
            placeholder="Search by text",
            label=dmc.Text("Text", c="dimmed", fw=300),
            variant="subtle",
            leftSection=DashIconify(
                icon="material-symbols-light:text-ad-outline", color="#0097b2", width=20
            ),
        )

    @staticmethod
    def filter_by_archive():
        return dmc.MultiSelect(
            placeholder="Search by archive",
            id="archive",
            label=dmc.Text("Archive", c="dimmed", fw=300),
            checkIconPosition="right",
            hidePickedOptions=True,
            maxDropdownHeight=200,
            searchable=True,
            clearable=True,
            leftSectionPointerEvents="none",
            leftSection=DashIconify(icon="bi-book", color="#0097b2"),
            variant="subtle",
            data=list(Archives),
            comboboxProps={"transitionProps": {"transition": "pop", "duration": 200}},
        )

    @staticmethod
    def filter_by_date():
        date = dmc.DatePickerInput(
            id="date",
            label=dmc.Text("Date", c="dimmed", fw=300),
            valueFormat="DD/MM/YYYY",
            allowSingleDateInRange=False,
            value=DBConnector.get_min_max_dates(engine, DBConnector.TABLE),
            type="range",
            leftSection=DashIconify(icon="uiw:date", color="#0097b2"),
            clearable=False,
            variant="subtle",
        )
        return date

    @staticmethod
    def filter_by_tag():
        tags = DBConnector.get_tags(engine, DBConnector.TABLE)
        tags = [tag.title() for tag in tags if tag and not tag.isnumeric()]
        select = dmc.Select(
            id="tag",
            data=tags,
            label=dmc.Text("Topic", c="dimmed", fw=300),
            placeholder="Search by topic",
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
    def get_switches():
        sorting = dmc.ActionIcon(
            DashIconify(icon="ic:outline-swap-vert", width=20),
            id="asc_desc",
            color="#ff5757",
            variant="subtle",
        )
        sorting = dmc.Tooltip(
            sorting,
            label="Inverse date order",
            multiline=True,
            withArrow=True,
            openDelay=3,
            position="top",
        )
        null_img = dmc.ActionIcon(
            DashIconify(icon="ic:sharp-image-not-supported", width=20),
            id="null_img",
            color="#ff5757",
            variant="subtle",
        )
        null_img = dmc.Tooltip(
            null_img,
            label="Eliminate empty images",
            multiline=True,
            withArrow=True,
            openDelay=3,
            position="top",
        )
        return dmc.Group([sorting, null_img], gap="xl", justify="center")

    @staticmethod
    def get_navbar():
        date = Layout.filter_by_date()
        tag = Layout.filter_by_tag()
        text = Layout.filter_by_text()
        archives = Layout.filter_by_archive()
        switches = Layout.get_switches()
        return dmc.Paper(
            dmc.Stack(
                [switches, date, tag, text, archives],
                align="stretch",
                justify="space-between",
                gap="xl",
            ),
            p=20,
            shadow="xs",
            h="100%",
        )

    @staticmethod
    def get_alert():
        return dmc.Container(
            dmc.Alert(
                dmc.Text(
                    "We didn't find any data with your current filters.",
                    c="dimmed",
                    fw=300,
                ),
                title=dmc.Text("Warning", fw=500),
                color="red",
            )
        )

    @staticmethod
    def get_stats(df, order):
        order = order if order else "reversed"
        stats_bar = dmc.Grid(
            [
                dmc.GridCol(Graph.get_graph(df, order), span=12),
            ],
            justify="center",
            align="flex-end",
        )
        return stats_bar

    @staticmethod
    def get_card(rowid, byte_img, title, content, tag, archive, date, link):
        img_height = 200
        src = (
            resize_image_for_html(byte_img, target_height=img_height)
            if byte_img
            else "https://placehold.co/600x400?text=Placeholder"
        )
        date = datetime.strftime(date, "%B, %d %Y")
        archive = archive.strip().title() if archive else archive
        tag = tag.strip().title() if tag else "None"
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
                    dmc.Image(fallbackSrc=src, radius=2, h=img_height),
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
                                        width=25,
                                        color="#ff5757",
                                        style={
                                            "position": "absolute",
                                            "top": 15,
                                            "left": 15,
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
                                    pb=15,
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
    def get_carousel_slides(args):
        return [
            dmc.CarouselSlide(Layout.get_card(*args[i]), style={"width": "33.33%"})
            for i in range(len(args))
        ]

    @staticmethod
    def get_carousel(args):
        carousel = dmc.Carousel(
            Layout.get_carousel_slides(args),
            slideSize="33.33%",
            w="100%",
            id="carousel",
            slideGap="md",
            loop=False,
            align="start",
            initialSlide=0,
            dragFree=False,
            slidesToScroll=Layout.SLIDES,
            nextControlIcon=DashIconify(
                icon="material-symbols:navigate-next", color="#0097b2"
            ),
            previousControlIcon=DashIconify(
                icon="material-symbols:navigate-before", color="#0097b2"
            ),
            withIndicators=True,
            classNames={
                "indicator": "dmc-indicator",
                "control": "dmc-control",
                "controls": "dmc-controls",
                "root": "dmc-root",
            },
        )
        return carousel

    @staticmethod
    def get_main(df, args, order):
        stats = Layout.get_stats(df, order)
        carousel = Layout.get_carousel(args)
        main = dmc.Stack([stats, carousel], gap="xl", align="stretch")
        return main

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
        footer = Layout.get_footer()

        appshell = dmc.AppShell(
            [
                dcc.Store(id="previous_active", data=0),
                dcc.Store(
                    id="last_seen",
                    data={DBCOLUMNS.rowid: "", DBCOLUMNS.date: None, "direction": None},
                ),
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
                dmc.AppShellMain(dmc.Container(id="main", size="xl", p="xl")),
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
