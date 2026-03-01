import pandas as pd
import asyncpg as apg
import asyncio
import plotly.express as px


async def connect_to_questdb():
    conn = await apg.connect(
        host='127.0.0.1',
        port=8812,
        user='admin',
        password='quest',
        database='qdb'
    )
    return conn

async def main():
    conn = await connect_to_questdb()

    try:
        query = (f"SELECT timestamp, speed, distance, driver, race, year from "
                 f"test_telemetry where lap_number = 1.0 AND driver in ('HAM', 'VER') "
                 f"AND race='British Grand Prix' AND year in (2023, 2024)")
        values = await conn.fetch(query)
        df = pd.DataFrame(values)
        df.columns = ["timestamp", "speed", "distance", "driver", "race", "year"]
        # fig = px.line(df, x="distance", y="speed", color="driver")
        fig = px.line(df, x="distance", y="speed", color=["year", "driver", "race"])
        fig.show()
    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())