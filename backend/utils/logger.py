import logging


def get_logger(name, log_level=logging.INFO):
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("app.log"), logging.StreamHandler()],
    )

    # Create and return a logger instance with the specified name
    return logging.getLogger(name)
