
from classes.price_data.price_data import PriceData

import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

plt.show(block=True)
plt.interactive(False)

# get data
eurusd = PriceData('eurusd')
eurusd.fetch_query("select * from `ads.eurusd` where period = 240 and date(time) > '2016-10-01'")

plt.plot(eurusd.data['close'])
plt.show()
plt.close()

# create signal
eurusd.data['signal'] = (eurusd.close() - eurusd.open()) / (eurusd.high() - eurusd.low() + 0.01)
eurusd.data['lagged_signal'] = eurusd.data['signal'].shift(periods=1)

plt.plot(eurusd.data['signal'])

# lag signal
plt.plot(eurusd.slice('2019-04-01', '2019-05-12')['lagged_signal'])
plt.show()
plt.close()


# downsample to daily
agg_rules = {
    'open' : 'first',
    'high' : 'max',
    'low' : 'min',
    'close' : 'last'
}

eurusd_daily = eurusd.data.resample('1D').agg(agg_rules)

plt.plot(eurusd_daily)
plt.show()
plt.close()
