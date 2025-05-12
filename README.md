# Archives Collection

Archives Collection is a Python-based project designed to collect, visualize and filter data from news archives websites. The system is built with a robust, modular architecture that leverages multiple design patterns to ensure scalability and maintainability. It has been proven at scale by successfully collecting millions of data points, opening the door to many AI and advanced analytics applications.

> **Important:**
> The collected data is subject to copyright laws. This project is intended for educational and research purposes only. You must ensure that your use of any data complies with all applicable copyright regulations. The author does not encourage any breach of copyright.

## Table of Contents

- [Technologies](#technologies)
- [Installation](#installation)
- [Modules](#modules)
  - [Data Collection](#data-collection)
  - [Frontend](#frontend)
  - [Backend](#backend)
  - [Database](#database)


## Technologies

- **Programming Language:** Python
- **Data Collection:** Cloudscaper, Beautiful Soup
- **Containerization:** Docker, Docker Compose, Makefile (for container management)
- **Front End:** Dash, Dash Mantine Components
- **Back End:** Dash, Celery, Redis
- **Database:** PostgreSQL with SQLAlchemy
- **Search Engine:** tsvector for text search, pgvector for semantic search
- **Embedding Model**: Jina v3 for textual multilingue embeddings.

## Installation

Before running this project, make sure you have Docker and Docker Compose installed (see the [official Docker documentation](https://docs.docker.com/engine/install/ubuntu/)). Additionally, install GNU Make, Python 3, and the PostgreSQL client using the following commands:

```bash
sudo apt update && sudo apt install -y make python3 python3-pip postgresql-client
```

Moreover, to use the provided embedding service, you’ll need an NVIDIA GPU. If no GPU is detected, the embedding column will remain `None` by default. The setup process will attempt to detect your system configuration and install the appropriate versions automatically, but you can override these settings in the next step if needed.

> Note: Building the embedding service may take a considerable amount of time, as it involves installing vLLM, CUDA, PyTorch, and downloading Jina model weights.


1. **Clone the Repository:**

   ```bash
   git clone https://github.com/issahammoud/archives_collection.git
   cd archives_collection

2. **Configure Environment Variables**

    Copy the sample file and fill in your own settings:

    ```bash
    cp .env.template .env
    ```
    Then open `.env` and enter your database credentials and any other required configuration values.

    On first startup, these settings will be used to automatically create and configure your database and its user. The `HNSW_EF_SEARCH` value controls the HNSW algorithm's search parameter; higher numbers improve recall but increase query time.

3. **Build and Run Containers:**

    Run the following to build and start the app:

    ```bash
    make build [EMBEDDING_MODE=<GPU|NONE>]
    make
    ```

    When you run `make build`, the system will automatically detect whether a GPU is available and install the appropriate dependencies. If you don’t need embedding vectors, you can speed up the process by forcing a lightweight build with `make build EMBEDDING_MODE=NONE`.

    After the initial build completes you only need to use `make` to start the service. Then open your browser to [http://localhost:8050](http://localhost:8050) to access the interface.

4. **Stopping and Cleaning Up:**

    To stop the containers:

    ```bash
    make stop
    ```

    To remove containers and images (this will not remove the mounted volumes):

    ```bash
    make clean
    ```
5. **Other useful commands:**

    To run a jupyter notebook:

    ```bash
    make jupyter
    ```
    To show the containers logs:

    ```bash
    make logs
    ```

## Modules

### Data Collection

Code and documentation in src/data_scrapping

- **Scraping Engine:**
  Uses Cloudscaper to fetch data from news archives.

- **Design Patterns:**
  Implements Decorator, Registry, Strategy, and Factory patterns for a clean, scalable codebase.

- **Collectors:**
  10 individual collectors can be aggregated for comprehensive data collection.

### Frontend

Code in src/helpers/layout.py and assets/styles.css

- **Carousel**: A carousel showing the collected articles with scrolling capabilities.

- **Navbar**: A navbar containing the control button for filtering and sorting:
  - start collection/stop collection
  - change order: to inverse the date order (ascending/descending)
  - filter image: show/remove articles with empty images (placeholders)
  - group by: show the date grouped per day/month/year
  - Filter by date/topic/text/archive.


- **Histogram**: A plotly graph showing the frequency of articles per day/month/year.

### Backend

Code in src/utils/callbacks.py and src/utils/celery_tasks.py

- **Dash Callbacks:**
  Orchestrate interactions between the front end and backend processes.

- **Celery & Redis:**
  Manage data collection task asynchronously.

### Database: (src/helpers/db_connector)
  - Use Postgres and pgvector extension as a vector db.
  - Fetch data for scrolling using keyset pagination.
  - Apply filters dynamically to sync with the interface.


**Table:** `articles`

| Column            | Type                              | Constraints / Generation                                                                                                       |
|-------------------|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `rowid`           | `BIGINT`                          | Primary Key, auto-increment                                                                                                    |
| `date`            | `DATE`                            | Not Null                                                                                                                       |
| `archive`         | `VARCHAR`                         | Not Null                                                                                                                       |
| `image`           | `TEXT`                            | Nullable                                                                                                                       |
| `title`           | `VARCHAR`                         | Nullable                                                                                                                       |
| `content`         | `VARCHAR`                         | Nullable                                                                                                                       |
| `tag`             | `VARCHAR`                         | Nullable                                                                                                                       |
| `link`            | `VARCHAR`                         | Not Null                                                                                                                       |
| `hash`            | `BIGINT`                          | Computed as `hashtext(link)::BIGINT`, persisted; Not Null; Unique                                                              |
| `embedding`       | `HALFVEC(1024)`           | stores vector embeddings in HALFVEC format                                                                           |
| `text_searchable` | `TSVECTOR`                        | Generated as `to_tsvector('french', coalesce(title, '') \|\| ' ' \|\| coalesce (content, ''))` |


**Indexes**

| Name                    | Columns            | Type   | Options                 |
|-------------------------|--------------------|--------|-------------------------|
| `date_rowid_index`      | `date, rowid`      | B-Tree |                         |
| `text_searchable_index` | `text_searchable`  | GIN    |                         |
| `embedding_index`       | `embedding`        | HNSW   | `m=32, ef_construction=128` |


---

*Happy coding!*
