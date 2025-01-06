import warnings
from src.utils.logging import logging
from src.utils.parser import get_config
from src.data_scrapping.collector_factory import CollectorFactory

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


if __name__ == "__main__":
    config = get_config()

    collector = CollectorFactory(
        config.archives,
        2 * len(config.archives),
        begin_date=config.begin_date,
        end_date=config.end_date,
        timeout=config.timeout,
    )

    collector.run()
