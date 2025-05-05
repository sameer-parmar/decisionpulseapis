def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None