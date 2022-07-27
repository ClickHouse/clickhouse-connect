import logging

logger = logging.getLogger(__name__)

try:
    import orjson as json_impl
    logger.info('Using orjson as the JSON implementation')
except ImportError:
    try:
        import ujson as json_impl
        logger.info('Using ujson as the JSON implementation')
    except ImportError:
        import json as json_impl
        logger.info('Using default JSON implementation')



