## Tasks

When creating functions and plots, keep in mind that these should be easy to
include in the dashboard we create in part 4 of the project. This means that
after connections to the databases have been established, all data download,
preparation, modelling and plotting must be done in functions that are called by
events in the dashboard (buttons, dropdown menus, checkboxes, clicking on maps,
etc.) without changing code. If there is more than one time format/zone, choose
wisely and/or convert to a common format when combining data.

### Pivoting

Use the consumption table from "part 1" of the project work for the pivoting
having consumption in kWh as the response.

- [x] Make a function that takes this table as input and lets the user choose between grouping/pivoting municipalities or "industry groups"/"Branche", combined with a choice of aggregation function, e.g., mean, max, ...
  - [x] Create a corresponding plot function displaying the results, e.g., as a barplot. Apply the function for both grouping choices above.
  - [x] Display the results numerically in addition to the plots.
- [x] Create a plot function for a (side-by-side) grouped barplot (see, e.g., Plotly histogram) where one can choose if municipalities or "industry group" should be the grouping factor and the opposite ("industry group" or municipality) should be bars within the group. Apply the function with both choices.

### Correlation

In "part 2" of the project work you were asked to "Plot the purchase price of gas in DKK" using raw data and the DCT-filtered data. You also downloaded weather data for all municipalities.

- [x] Create a function that combines DCT-filtering and the Sliding Window Correlation (SWC) approach for these data. Check that your data series are in sync before computing correlations. Inputs:
  - [x] The series of purchase prices.
  - [x] The series of average windspeed across all municipalities.
  - [x] Logical (True/False) to turn on/off filtering.
  - [x] Cut-off value for the DCT-filter.
  - [x] Width of the SWC window.
  - [x] Lag added to the windspeed series.
- [x] Plot the results as a figure with two subplots stacked vertically.
  - [x] The top plot should contain the purchase prices and (lagged) windspeeds with the corresponding y-axis on the left and right. (Same plot, two y-axes)
  - [x] The bottom plot should contain the SWC.
  - [x] Discuss the figure and strategy to produce it.

### Forecasting

- [ ] Create a function that fits a (S)ARIMAX model with the kWh of a selectable "ProductionType" as the response in a selectable municipality of choice. Use temperature and windspeed from the same municipality as exogenous variables (if chosen, see below). Perform one-step-ahead and dynamic forecasting from a user-selected timepoint (cut-off between training and test period). The function should return both the model and the predictions.
  - [ ] Input parameters to the function above: Which "ProductionType", which municipality, inclusion of temperature, inclusion of windspeed and cut-off time between training and testing.
  - [ ] Search for good p,d,q,P,D,Q,s parameter values for a single choice of "ProductionType", municipality and possibly exogenous variables. Print the summary of the optimal model. Plot the one-step-ahead and dynamic prediction for a chosen time cut-off.

### Ekstra

- [ ] Fikse marginene i 'Correlation' plottet.

### Notater

SARIMAX(1,1,1)(2, 1, 2, 24) best AIC og loglike best for "thermalpowermwh"

