from decimal import Decimal

TWOPLACES = Decimal("0.01")


def format_amount(value: Decimal) -> str:
    return f"{Decimal(value).quantize(TWOPLACES):.2f}"
