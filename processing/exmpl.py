import pandas as pd
import datetime
import numpy as np
from fbprophet import Prophet
from matplotlib import pyplot


sz = 100

data = np.concatenate([np.full(sz // 4, 1.5),
                       np.full(sz // 4, 8.5),
                       np.full(sz // 4, 1.5),
                       np.full(sz // 4, 3.0)])
data += np.random.randn(sz)
data += np.sin(np.arange(sz) * 0.1)
# data += np.sin(np.arange(sz) * 0.03) * 6
# data += np.sin(np.arange(sz) * 0.05) * 7
data = np.abs(data)
data = np.cumsum(data)
data = data / 500 * 85

step = datetime.timedelta(seconds=3600)
dates = [datetime.datetime.now() + step * i for i in range(sz)]
futures = [datetime.datetime.now() + step * i for i in range(sz, sz + 50)]

df = pd.DataFrame({'ds': np.array(dates), 'y': data})
m1 = Prophet()
m1.fit(df)
forecast_data = m1.predict(pd.DataFrame({"ds": np.array(futures)}))
f = m1.plot(forecast_data, xlabel="disk usage", yhat_label="OSD1")

df = pd.DataFrame({'ds': np.array(dates), 'y': data * 0.7})
m2 = Prophet()
m2.fit(df)
forecast_data2 = m2.predict(pd.DataFrame({"ds": np.array(futures)}))
m2.plot(forecast_data2, ax=f.axes[0], yhat_label="OSD2")

f.axes[0].plot([dates[0], futures[-1]], [85, 85], '--', color="red", linewidth=1)
for idx, val in enumerate(forecast_data['yhat']):
    if val > 85:
        break

date = forecast_data['ds'][idx - 1]
f.axes[0].plot([date, date], [85, 0], '--', color="red", linewidth=1)
f.axes[0].text(date, -5, "{:%Y-%m-%d}".format(date))
f.axes[0].legend()

pyplot.show()
