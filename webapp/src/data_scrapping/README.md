# Data Collection

This directory contains the code used to collect articles from various news websites that expose archived content organized by date.

Most news sites offer a structured archive, often accessible via predictable date-based URLs. By following these patterns, we can generate the list of archive pages to visit and extract articles from them.

Each website requires a few configuration details:

- Base URL
- Date format
- Start and end dates for available archives

The main difference between websites lies in how article content is extracted from the HTML, which is handled through customized scraping logic.

## Handling Pagination and Duplicate Avoidance

Some websites paginate articles for a single day. To handle this, we use a decorator to dynamically update the list of article URLs discovered on each archive page.

We also use another decorator to skip dates that have already been processed and stored in the database. This is necessary to avoid redundant work and is implemented using the Decorator pattern.

However, checking if a date has been "collected" isn't always straightforward. Simply verifying that a date exists in the database can lead to incorrect assumptions—for instance, if only a few articles were collected before an early interruption. To address this, we use a heuristic:

> If the number of collected articles for a day is less than 90% of the median count across other days, we consider it incomplete and re-collect that date.

Thanks to a unique constraint on the article URL hash in the database, duplicate entries are automatically avoided.

## Strategy Pattern for Fetching

To make the request mechanism flexible, we use the Strategy pattern. Initially, both `requests` and `selenium` were supported, allowing dynamic switching between fetching strategies. Later, Selenium was removed due to its overhead and because sufficient data could be collected without it.

Currently, the project uses `cloudscraper`—a wrapper around requests that handles Cloudflare protections automatically.

## Extensibility
To add support for a new website, simply extend the `DataCollector` class. Minimal boilerplate is needed.

All collectors are registered using a `Registry` class, which functions as both a registry and a simple factory. This makes it easy to run all collectors or a subset of them, depending on your use case.

For parallel execution, the `CollectorsAggregator` class instantiates and runs all registered collectors concurrently.

## Class Diagram

You can see here the relationship between each class in the following class diagram. The entrypoint to the code is the `CollectorsAggregator` class. It is called in `celery` (src/utils/celery_tasks.py):

![Data Collection Class Diagram](/webapp/assets/data_collection_diagram.png)