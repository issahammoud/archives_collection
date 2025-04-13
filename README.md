# Archives Collection

Archives Collection is a Python-based project designed to collect and visualize data from news archives websites. The system is built with a robust, modular architecture that leverages multiple design patterns to ensure scalability and maintainability. It has been proven at scale by successfully collecting millions of data points, opening the door to many AI and advanced analytics applications.

> **Important:**
> The collected data is subject to copyright laws. This project is intended for educational and research purposes only. You must ensure that your use of any data complies with all applicable copyright regulations. The author does not encourage any breach of copyright.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Installation](#installation)
- [Usage](#usage)
- [Modules](#modules)
  - [Data Collection](#data-collection)
  - [Visualization](#visualization)
  - [Backend](#backend)
  - [Database](#database)
- [Future Work](#future-work)
- [License](#license)

## Overview

Archives Collection automates the process of collecting and visualizing news archives data. By combining web scraping, asynchronous processing, and interactive dashboards, this project provides a platform that not only aggregates a large volume of data but also enables efficient analysis and search.

## Features

- **Automated Data Collection:** Uses Cloudscaper and Selenium with a clean, modular architecture.
- **Modular Design:** Implements multiple design patterns (Decorator, Registry, Strategy, Factory) for flexibility and scalability.
- **Collector Aggregator:** Integrates 16 distinct collectors into a unified aggregation system.
- **Interactive Visualization:** Built with Dash and Dash Mantine Components for a modern, responsive user interface.
- **Asynchronous Processing:** Leverages Dash callbacks and Celery (with Redis) to manage long-running tasks.
- **Advanced Search:** Uses PostgreSQL with SQLAlchemy and tsvector for efficient full-text search.
- **Containerized Deployment:** Managed via Makefile, Docker, and Docker Compose for easy build, run, stop, and cleanup operations.


## Technologies

- **Programming Language:** Python
- **Data Collection:** Selenium, Cloudscaper
- **Containerization:** Docker, Docker Compose, Makefile (for container management)
- **Web Framework:** Dash, Dash Mantine Components
- **Asynchronous Processing:** Celery with Redis as the message broker
- **Database:** PostgreSQL, SQLAlchemy, tsvector

## Installation

Before running this project, make sure you have Docker and Docker Compose installed (see the [official Docker documentation](https://docs.docker.com/engine/install/ubuntu/)). Additionally, install GNU Make, Python 3, and the PostgreSQL client using the following commands:

```bash
sudo apt update && sudo apt install -y make python3 python3-pip postgresql-client
```

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/issahammoud/archives_collection.git
   cd archives_collection

2. **Configure Environment Variables**

    Create or update a `.env` file with your database credentials and other configuration settings. It should include the following variables:
    ```
    POSTGRES_USER=
    POSTGRES_PASSWORD=
    POSTGRES_DB=
    ```
    Those variables will be automatically used the first time to create and set the database and the user.

3. **Build and Run Containers:**

    ```bash
    make build
    make
    ```

    Or use Docker Compose directly:

    ```bash
    docker compose up --build
    ```

4. **Stopping and Cleaning Up:**

    To stop the containers:

    ```bash
    make stop
    ```

    To remove containers and volumes:

    ```bash
    make clean
    ```

## Usage

- **Data Collection:**
  The data collection process is automated via Cloudscaper and Selenium. The collectors are aggregated into a single system that processes multiple sources concurrently.

- **Visualization:**
  Once the data is collected, access the interactive dashboard at [http://localhost:8050](http://localhost:8050) to explore visualizations and start collecting data.

- **Background Processing:**
  Long-running tasks (e.g., data collection) are handled asynchronously using Celery with Redis.

- **Search:**
  Utilize the PostgreSQL-backed full-text search powered by SQLAlchemy and tsvector to quickly query large datasets.

## Modules

### Data Collection

- **Scraping Engine:**
  Uses Selenium and Cloudscaper to fetch data from news archives.

- **Design Patterns:**
  Implements Decorator, Registry, Strategy, and Factory patterns for a clean, scalable codebase.

- **Collectors:**
  16 individual collectors can be aggregated for comprehensive data collection.

### Visualization

- **Dash Dashboard:**
  An interactive front end built with Dash and enhanced with Dash Mantine Components for a modern user interface.

- **Real-Time Updates:**
  Dynamic dashboards that update as new data is collected.

### Backend

- **Dash Callbacks:**
  Orchestrate interactions between the front end and backend processes.

- **Celery & Redis:**
  Manage long-running tasks asynchronously.

### Database

- **PostgreSQL:**
  Stores the collected data.

- **SQLAlchemy:**
  Serves as the ORM for database interactions.

- **TSVECTOR:**
  Enables efficient full-text search capabilities.

## Future Work

- **AI and Advanced Analytics:**
  Dedicated modules for applying AI techniques and advanced analytics to the collected data.

- **Enhanced Visualizations:**
  Additional dashboard components and interactive features to further improve data exploration.

- **Scalability Enhancements:**
  Further optimizations in the data collection and aggregation pipelines to handle even larger datasets.


## License

This project is licensed under the [MIT License](LICENSE).

---

*Happy coding!*
