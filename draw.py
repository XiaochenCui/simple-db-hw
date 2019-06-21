import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

def get_transactions():
    pass

from collections import namedtuple
Transaction = namedtuple("Transaction", ["tid",
                                         "pageid",
                                         "lock",
                                         "start_time",
                                         "end_time",
                                         "result",
                                         ])

t1 = Transaction(
    "transaction.45",
    "heapPageId.-752505997.0",
    "SHARED_LOCK",
    "11:12:03,241",
    "11:12:03,242",
    "success",
)
t2 = Transaction(
    "transaction.44",
    "heapPageId.-752505997.0",
    "EXCLUSIVE_LOCK",
    "11:12:03,229",
    "11:12:03,229",
    "success",
)

print(pd.to_datetime(t1.start_time, format="%H:%M:%S,%f"))

times = [
    pd.to_datetime(t1.start_time, format="%H:%M:%S,%f"),
    pd.to_datetime(t2.start_time, format="%H:%M:%S,%f"),
]

names = [
    "thread-1",
    "thread-2",
]

index = names

data = {
    "d": times,
}

df = pd.DataFrame(data=data,index=index)

plot = df.plot(
    kind='barh',
    # legend=None,
    # figsize=(12,8), stacked=True,
    title="transaction waterfall",
)

plot.get_figure().savefig("waterfall.png",dpi=200,bbox_inches='tight')

#Use python 2.7+ syntax to format currency
def money(x, pos):
    'The two args are the value and tick position'
    return "${:,.0f}".format(x)
formatter = FuncFormatter(money)


def draw():
    #Data to plot. Do not include a total, it will be calculated
    index = ['sales','returns','credit fees','rebates','late charges','shipping']
    data = {'amount': [350000,-30000,-7500,-25000,95000,-7000]}

    index = ['thread-1', 'thread-2']
    from datetime import datetime
    data = {'time':
            pd.date_range('2018-01-01', periods=2, freq='H')
            }

    #Store data and create a blank series to use for the waterfall
    trans = pd.DataFrame(data=data,index=index)
    # print(trans)
    # print(trans.T)
    # blank = trans.amount.cumsum().shift(1).fillna(0)

    #Get the net total number for the final element in the waterfall
    # total = trans.sum().amount
    # trans.loc["net"]= total
    # blank.loc["net"] = total

    #The steps graphically show the levels as well as used for label placement
    # step = blank.reset_index(drop=True).repeat(3).shift(-1)
    # step[1::3] = np.nan

    #When plotting the last element, we want to show the full bar,
    #Set the blank to 0
    # blank.loc["net"] = 0

    #Plot and label
    my_plot = trans.plot(kind='barh',
                           # stacked=True,
                         # bottom=blank,
                         legend=None,
                         # figsize=(10, 5),
                         title="2014 Sales Waterfall")
    # print(step)
    # print()
    # print(step.index)
    # print()
    # print(step.values)
    # my_plot.plot(step.index, step.values,'k')
    # my_plot.set_xlabel("Transaction Types")

    #Format the axis for dollars
    # my_plot.yaxis.set_major_formatter(formatter)

    #Get the y-axis position for the labels
    # y_height = trans.amount.cumsum().shift(1).fillna(0)

    #Get an offset so labels don't sit right on top of the bar
    # max = trans.max()
    # neg_offset = max / 25
    # pos_offset = max / 50
    # plot_offset = int(max / 15)

    #Start label loop
    # loop = 0
    # for index, row in trans.iterrows():
        # print(index)
        # print()
        # print(row)
        # print()
        # # For the last item in the list, we don't want to double count
        # if row['amount'] == total:
            # y = y_height[loop]
        # else:
            # y = y_height[loop] + row['amount']
        # # Determine if we want a neg or pos offset
        # if row['amount'] > 0:
            # y += pos_offset
        # else:
            # y -= neg_offset
        # my_plot.annotate("{:,.0f}".format(row['amount']),(loop,y),ha="center")
        # loop+=1

    #Scale up the y axis so there is room for the labels
    # my_plot.set_ylim(0,blank.max()+int(plot_offset))
    #Rotate the labels
    # my_plot.set_xticklabels(trans.index,rotation=0)
    my_plot.get_figure().savefig("waterfall.png",dpi=200,bbox_inches='tight')


if __name__ == "__main__":
    pass
    # draw()
