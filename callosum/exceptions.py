class CallosumError(Exception):
    pass


class ClientError(CallosumError):
    pass


class ServerError(CallosumError):
    pass


class HandlerError(CallosumError):
    pass
