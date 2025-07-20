## Data to Decision dashboard project

This is the final project of my "Data to Decision"-course where we built a
dashboard displaying energy production and weather forecasts in Denmark 2022.
Since the delivery of the project many of the dashboard's functionalities have
become broken, reflected in the screenshots below.

### Front page

The main page consist a map of Denmark and a plot of an aspect of the data
source. We can choose data source – Energy Production per Municipality,
Energy Production and Consumption in the electricity production areas,
Consumption in Industry, Public and Private per Municipality and weather
forecast – and specify what we want to visualize from the particular
source.

![[mainpage_1]](imgs/homepage_1.jpeg)

![[mainpage_2]](imgs/homepage_2.jpeg)

### Sliding Window Correlation

The correlation page visualize the sliding window correlation between gas
purchase price and the average wind speed across all municipalities. The
page features a toggle to enable/disable DCT-filtering and change the width of
the sliding window.

![[correlation]](imgs/dct_swc_correlation.jpeg)

### Pivoting and aggregation

The summary page shows the overall consumption for the different municipalities
in the different industry groups (public, private and industry), and conversely
showing the consumption for every municipality in each industry group.
The data is grouped/pivoted with a chosen aggregation method.

![[summary]](imgs/summary_page_broken.jpeg)

### Forecasting

The forecasting page features the possibility to forecast a type of production,
as the endogenous variable, in a selectable municipality of choice using a
SARIMAX model. Optionally, we can include exogenous variables such as the
temperature and wind speed in the forecasting. We can specify a cutoff point
from which we want to start forecasting from. We perform both one-step-ahead and
dynamic forecasting and the results are plotted below and display the model
parameters.

![[forecasting]](imgs/broken_forecasting.jpeg)
