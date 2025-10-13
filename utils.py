def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
