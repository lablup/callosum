import yarl


def redis_addr_to_url(
    value: str | tuple[str, int],
    *,
    scheme: str = "redis",
) -> str:
    match value:
        case str():
            url = yarl.URL(value)
            if url.scheme is None:
                return str(yarl.URL(value).with_scheme(scheme))
            return value
        case (host, port):
            return f"{scheme}://{host}:{port}"
        case _:
            raise ValueError("unrecognized address format", value)
