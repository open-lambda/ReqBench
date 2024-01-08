import pandas as pd

df = pd.read_csv("docker_metrics.csv")

df2 = pd.DataFrame()
df2["total"] = df["received"] - df["start"]
df2["create"] = df["create_done"] - df["req"]
df2["import"] = df["end_import"] -  df["start_import"]

df2.to_csv("docker_time.csv")

print(
    {
        "avg_total (ms)" : df2["total"].mean(),
        "avg_create (ms)" : df2["create"] .mean(),
        "avg_create (%)" : df2["create"].mean()/df2["total"].mean() * 100, 
        "avg_import (ms)" : df2["import"].mean(),
        "avg_import (%)" : df2["import"].mean()/df2["total"].mean() * 100,
    }
)