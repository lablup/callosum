def redis_addr_to_url(
    value: str | tuple[str, int],
    *,
    scheme: str = "redis",
) -> str:
    match value:
        case str():
            return f"{scheme}://{value}"
        case (host, port):
            return f"{scheme}://{host}:{port}"
        case _:
            raise ValueError("unrecognized address format", value)
