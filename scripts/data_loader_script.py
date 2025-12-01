import fastf1
import requests
import json
import asyncio
import aiohttp


base_url = "http://localhost:8000"
resource = "/session/load"


async def make_request(async_session, year, event):
    request_body = []
    sessions = ["Session1", "Session2", "Session3", "Session4", "Session5"]
    race = event['EventName']
    for session in sessions:
        session_name = event.get(session)
        if session_name == 'None':
            continue

        request_body.append({
            "year":  year,
            "race": race,
            "session": session_name
        })

    async with async_session.post(base_url + resource, json=request_body) as response:
        response.raise_for_status()

async def main():
    event_schedule = fastf1.get_event_schedule(2023)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, event in event_schedule.iterrows():
            task = make_request(session, event.year, event)
            tasks.append(task)
        await asyncio.gather(*tasks)

    print("All requests complete")

if __name__ == "__main__":
    asyncio.run(main())
