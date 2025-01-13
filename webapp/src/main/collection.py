import warnings
from src.utils.logging import logging
from src.utils.parser import get_config
from src.data_scrapping.collectors_agg import CollectorsAggregator

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


if __name__ == "__main__":
    config = get_config()

    collector = CollectorsAggregator(
        begin_date=config.begin_date,
        end_date=config.end_date,
        timeout=config.timeout,
    )

    collector.run()
