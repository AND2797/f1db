import pyarrow.flight as flight
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('TkAgg')

def query_sql(client, query):
    ticket = flight.Ticket(query.encode("utf-8"))
    reader = client.do_get(ticket)
    df = reader.read_all().to_pandas()
    return df

client = flight.connect("grpc://localhost:8815")

tables = {'2018':'australia_qualifying_2018.laps', '2019':'australia_qualifying_2019.laps',
          '2020':'australia_qualifying_2020.laps', '2021':'australia_qualifying_2021.laps',
          '2022':'australia_qualifying_2022.laps', '2023':'australia_qualifying_2023.laps',
          '2024':'australia_qualifying_2024.laps', '2025':'australia_qualifying_2025.laps'}

will_query = "SELECT * FROM {0} where Team in ('Williams') ORDER BY LapTime ASC LIMIT 1"
fast_query = "SELECT * FROM {0} ORDER BY LapTime ASC LIMIT 1"

williams = []
fastest = []
for year, race in tables.items():
    will_sql = will_query.format(race)
    will_df = query_sql(client, will_sql)
    will_df["Year"] = year
    will_df = will_df[["Year", "LapTime", "Team", "Driver"]]
    williams.append(will_df)

    all_sql = fast_query.format(race)
    all_df = query_sql(client, all_sql)
    all_df["Year"] = year
    all_df = all_df[["Year", "LapTime", "Team", "Driver"]]
    fastest.append(all_df)




williams_df = pd.concat(williams)
fastest_df = pd.concat(fastest)

data = [williams_df['LapTime'], fastest_df['LapTime']]

plt.boxplot(data, labels=['Williams', 'Fastest'])
plt.xlabel('Dataset')
plt.ylabel('Lap Time (s)')
plt.show()


# plt.plot(williams_df['Year'], williams_df['LapTime'], label='Williams')
# plt.plot(fastest_df['Year'], fastest_df['LapTime'], label='Fastest')
# plt.xlabel("Year")
# plt.ylabel("Lap Time (s)")
# plt.title("Lap Time over years")
# plt.show(block=True)


