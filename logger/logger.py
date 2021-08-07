import logging


class Logging:
    def __init__(self, name: str, mode: str = "w",
                 log_format: str = f"%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"):
        self.name = name
        self.mode = mode
        self.log_format = log_format

    def get_file_handler(self) -> logging:
        modify_name = self.name

        if self.name.split(".")[-1] != "log":
            modify_name = "{}.log".format(self.name)

        file_handler = logging.FileHandler("{}/{}".format("logs", modify_name), mode=self.mode)

        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(self.log_format))
        return file_handler

    def get_stream_handler(self) -> logging:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(logging.Formatter(self.log_format))
        return stream_handler

    def get_logger(self) -> logging:
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)
        logger.addHandler(self.get_file_handler())
        return logger
