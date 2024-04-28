from src.utils.logging import logging
from src.utils.parser import get_config
from src.data_processing.collector_factory import CollectorFactory

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    config = get_config()

    collector = CollectorFactory(
        config.archives,
        config.workers,
        begin_date=config.begin_date,
        end_date=config.end_date,
        timeout=config.timeout,
    )

    collector.run()
