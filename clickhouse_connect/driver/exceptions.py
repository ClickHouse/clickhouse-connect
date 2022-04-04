class ServerError(Exception):
    pass


class DriverError(Exception):
    pass


class NotSupportedError(DriverError):
    pass


class OperationError(Exception):
    pass