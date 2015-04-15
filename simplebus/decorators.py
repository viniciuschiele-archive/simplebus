from simplebus.state import current_bus


def consume(queue, max_delivery_count=3, endpoint=None):
    def decorator(cls_or_func):
        current_bus.consume(queue, cls_or_func, max_delivery_count, endpoint)
        return cls_or_func
    return decorator


def subscribe(topic, endpoint=None):
    def decorator(cls_or_func):
        current_bus.subscribe(topic, cls_or_func, endpoint)
        return cls_or_func
    return decorator