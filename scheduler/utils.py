from datetime import datetime, timedelta


def get_datetime_struct(timestamp):
    """
    Returns own date/time structure with all needed fields for scheduling
    Fields description (in order):
      * year - current year
      * month - current month (1-12
      * mweek - week number within current month (0-5 usually)
      * mday - day of month
      * last_mday - last day of current month
      * last_wday - last week of current month
      * wday - calendar day of the week (0 is Monday)
      * hour - current hour
      * min - current minute)
      * timestamp - time in seconds since epoch
    """
    current_time = datetime.fromtimestamp(timestamp)
    year, month, mday, hour, min, _, wday, _, _ = current_time.timetuple()
    mweek = current_time.isocalendar()[1] - datetime(year, month, 1).isocalendar()[1]
    last_mday = datetime(year, month+1, 1) - timedelta(days=1)
    last_mweek = last_mday.isocalendar()[1] - datetime(year, month, 1).isocalendar()[1]

    return (year, month, mweek, mday, last_mday.day, last_mweek, wday, hour, min)