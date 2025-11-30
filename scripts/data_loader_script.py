import fastf1
import requests
import json


base_url = "http://localhost:8000"
resource = "/session/load"


def make_request(year, event):
    request_body = []
    sessions = ["Session1", "Session2", "Session3", "Session4", "Session5"]
    race = event['EventName']
    for session in sessions:
        session_name = event[session]
        if session_name == 'None':
            continue

        request_body.append({
            "year":  year,
            "race": race,
            "session": session_name
        })

    requests.post(base_url + resource, json=request_body)

if __name__ == "__main__":
    event_schedule = fastf1.get_event_schedule(2024)
    event_schedule.apply(lambda event: make_request(2024, event), axis=1)
