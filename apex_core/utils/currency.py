"""Currency formatting utilities."""


def format_usd(cents: int) -> str:
    """
    Format a value in cents as USD string.
    
    Args:
        cents: Amount in cents (e.g., 1999 for $19.99)
    
    Returns:
        Formatted USD string (e.g., "$19.99")
    """
    dollars = cents / 100
    return f"${dollars:,.2f}"
