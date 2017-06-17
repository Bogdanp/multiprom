class Metric:
    """
    Parameters:
      name(str)
      description(str)
    """

    kind = None

    def __init__(self, collector, name, description):
        self.collector = collector
        self.name = name.encode("utf-8")
        self.description = description.encode("utf-8") if description else None
        self.register()

    def register(self):
        """Register this metric with its collector.
        """
        self.collector.send(encode(b"reg", self.kind, self.name, self.description))


class Counter(Metric):
    kind = b"counter"

    def inc(self, n=1, **labels):
        """Increment this counter by the given amount.

        Parameters:
          n(int or float): This must be a positive amount.
        """
        assert n >= 0, "amounts must be positive"
        self.collector.send(encode(b"inc", self.name, str(n), **labels))


def encode(operation, *args, **labels):
    """Encode a message so that it can be sent over the wire.

    Parameters:
      operation(str)
      \*args(tuple[str])
      \**labels(dict)

    Returns:
      bytes
    """
    if labels:
        args += (",".join(f'{name}="{labels[name]}"' for name in sorted(labels)),)

    message = operation
    for arg in args:
        message += b"\0"
        if arg:
            message += arg if isinstance(arg, bytes) else arg.encode("utf-8")

    message_len = str(len(message)).encode("ascii")
    return b"$" + message_len + b"\0" + message
