import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("omni2_all_years.dat",
                 delim_whitespace=True,
                 usecols=[0, 1, 2, 39, 40, 50],
                 names=["Year", "DOY", "Hour", "R", "Dst", "F10.7"])

print(df)
print(df["DOY"])
print(df.index)
df.index = pd.to_datetime(df["Year"] * 100000 + df["DOY"] * 100 + df["Hour"], format="%Y%j%H")
print(df.index)
df = df.drop(columns=["Year", "DOY", "Hour"])

print("Dataframe shape: ", df.shape)
dt = (df.index[-1] - df.index[0])
print("Number of hours between start and end dates: ", dt.total_seconds()/3600 + 1)

h, d, y = 24, 365, 55
print(f"{h} hours/day * {d} days/year * {y} years = {h*d*y} hours")

print("{} hours/day * {} days/year * {} years = {} hours".format(h, d , y, h*d*y))
print("%d hours/day * %d days/year * %d years = %d hours"%(h, d , y, h*d*y))

df.plot(figsize=(15,4))
df.plot(subplots=True, figsize=(15,6))
# df.plot(y=["R", "F10.7"], figsize=(15,4))
# df.plot(x="R", y=["F10.7", "Dst"], style='.')
plt.show()
