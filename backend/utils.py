import re
from datetime import datetime, timedelta

def validate_email(email):
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_phone(phone):
    """Validate phone number format"""
    pattern = r'^\+?1?\d{9,15}$'
    return re.match(pattern, phone) is not None

def format_date(date_str, format="%Y-%m-%d"):
    """Format date string"""
    if not date_str:
        return ""
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%B %d, %Y")
    except:
        return date_str

def get_today_date():
    """Get today's date as string"""
    return datetime.now().strftime("%Y-%m-%d")

def get_future_date(days):
    """Get future date"""
    future = datetime.now() + timedelta(days=days)
    return future.strftime("%Y-%m-%d")

def calculate_age(birth_date):
    """Calculate age from birth date"""
    if not birth_date:
        return None
    try:
        birth = datetime.strptime(birth_date, "%Y-%m-%d")
        today = datetime.now()
        age = today.year - birth.year
        if today.month < birth.month or (today.month == birth.month and today.day < birth.day):
            age -= 1
        return age
    except:
        return None

def sanitize_input(text):
    """Basic input sanitization"""
    if not text:
        return ""
    # Remove any potentially harmful characters
    return re.sub(r'[<>\'"]', '', str(text))