import logging

from src.core.logger import get_logger


def test_get_logger_returns_named_child_logger():
    logger = get_logger("src.orchestration.pipelines.run_prices")
    assert logger.name == "src.orchestration.pipelines.run_prices"


def test_root_src_logger_has_exactly_one_handler():
    get_logger("src.something")
    get_logger("src.something_else")

    root = logging.getLogger("src")
    assert len(root.handlers) == 1
    assert root.propagate is False


def test_formatter_includes_level_timestamp_and_module():
    get_logger("src.something")
    root = logging.getLogger("src")
    fmt = root.handlers[0].formatter._fmt
    assert "%(asctime)s" in fmt
    assert "%(levelname)" in fmt
    assert "%(name)s" in fmt
