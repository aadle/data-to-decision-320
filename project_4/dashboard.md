### 2024-11-18

The proposed layout is one page where you can navigate to other pages with the
help of a dropdown menu.

I mean, we've done the ground work, now the matter is integrating our work into
the dashboard. But kind of stupid that we've done all of this in notebooks and
we're now going to write in Python scripts?

How should we do this then? Interesting...

# Todo list

## Main page

-   [ ] Map of Denmark

    -   [ ] Production

        -   [ ] Dots at each municipality
        -   [ ] Selected production type, and municipality and day
        -   [ ] Scaled by sqrt(kWh)

    -   [ ] Consumption

        -   [ ] Dots at each municipality
        -   [ ] Selected industry, municipality and day
        -   [ ] Scaled by sqrt(kWh)

    -   [ ] Power import/export

        -   [ ] Large labelled dots placed naturally with regard to countries??
                Maybe change to marking the country?
        -   [ ] Default: Green. Current use 80% of year maximum: Yellow (both
                +/-). Current use (95%) of year maximum: Red (both +/-) ???

    -   [ ] Weather
        -   [ ] Dots at each municipality
        -   [ ] Scaled by size of weather property (normalised per property?)

-   [ ] Associated plot with chosen municipality from the map

    -   [ ] Production

        -   [ ] Dropdown menu for production type and municipality
        -   [ ] Plotting
            -   [ ] Graph original data in grey
            -   [ ] Smooth data in another color
            -   [ ] Mark current day, which is controlled by a slider

    -   [ ] Consumption

        -   [ ] Dropdown menu for production type and municipality
        -   [ ] Plotting
            -   [ ] Graph original data in grey
            -   [ ] Smooth data in another color
            -   [ ] Mark current day, which is controlled by a slider

    -   [ ] Power import/export

        -   [ ] Pie chart for each of the four countries that Denmark
                import/exports from/to. Pieces indicate current use and
                remainder up to the 2022 maximum.
        -   [ ] Color indicating import or export.

    -   [ ] Weather
        -   [ ] Dropdown menu for production type and municipality
        -   [ ] Plotting
            -   [ ] Graph original data in grey
            -   [ ] Smooth data in another color
            -   [ ] Mark current day, which is controlled by a slider

-   [ ] Day slider
-   [ ] Dropdown menu for choice data to be plotted
