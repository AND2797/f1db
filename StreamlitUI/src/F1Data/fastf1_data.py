import fastf1


if __name__ == "__main__":
    year = 2024
    schedule = fastf1.get_event_schedule(2024)
    print(schedule)