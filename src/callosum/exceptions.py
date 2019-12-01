class CallosumError(Exception):
    pass


class ConfigurationError(CallosumError):
    pass


class InvalidAddressError(CallosumError, ValueError):

    def __init__(self, invalid_param: str = None):
        if invalid_param:
            self.message =\
                f'''
                {invalid_param} either must not be specified
                or is invalid.
                '''
        else:
            self.message =\
                "An invalid address was specified."


class ClientError(CallosumError):
    pass


class ServerError(CallosumError):
    pass
