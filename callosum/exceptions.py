class CallosumError(Exception):
    pass


class ClientError(CallosumError):
    pass


class ParamError(ClientError):

    def __init__(self, error_param: str):
        self.message =\
            f'''
            {error_param} must not be specified in RedisStreamAddress,
            as objects using CommonStreamBinder are not supposed
            to be the consumers of any group.
            '''


class ServerError(CallosumError):
    pass


class HandlerError(CallosumError):
    pass
