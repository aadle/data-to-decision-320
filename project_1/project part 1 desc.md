Subject to minor revisions before the deadline.

## General

- All project work in IND320 should result in personal hand-ins.
- Co-operation is applauded and the use of AI tools is encouraged.
- Remember that there is a personal written exam at the end, so outsourcing without understanding implementations and documentation is a sub-optimal strategy.
  - Document your AI usage; if you use GitHub Copilot or similar to generate the code, include the query in the hand-in.
- Project works will build on each other, so laying a good foundation pays of in the long run.
- The final product is a Jupyter Notebook containing all the code needed to scrape, create tables, download data, and inject it into databases. It should be runnable from the first to the last cell and exported to HTML/PDF.
  - All code blocks must include comments.
  - All code blocks must be run before export so the messages and plots are shown.

## Tasks

### Remote database: MongoDB

- [x] Prepare a MongoDB account at mongodb.com.
- [x] Create a database.
- [x] Create two collections inside this database, for:
  - [x] Municipality data: "municipalities"
  - [x] Gas prices: "gas"

### Local database: Cassandra

- [x] Set up a new keyspace.

### Data scraping

- [x] Find the "List of municipalities of Denmark" on Wikipedia.
- [x] Use Python to extract the table on the webpage.
- [x] Convert the table to a Pandas DataFrame
- [x] Insert the data into the MongoDB collection "municipalities".

### API
- [x] Familiarise yourself with the API connection at https://www.energidataservice.dk/Lenker til en ekstern side.
- [x] Download data for the period 2022-01-01 to 2022-12-31 for the following attributes:
  - Remote database (MongoDB collection in parenthesis):
    - [x] Daily commercial gas prices ("gas").
  - Local database (Cassandra database table name in parenthesis):
    - [x] Hourly electricity production per municipality ("production").
    - [x] Hourly electricity consumption per municipality per industry, public and private ("consumption").
    - [x] Hourly electricity production and consumption per price area ("prodcons").

### Plotting
- [x] Import/export from/to Norway as a function of time.
- [x] Gas sale and purchase prices as a function of time.
- For the three electricity tables (production, consumption, prodcons):
  - [x] Extract the unique values/columns of:
    - [x] production: "MunicipalityNo" and "ProductionType"
    - [x] consumption: "MunicipalityNo" and "Branche"
    - [x] prodcons: "PriceArea" and "ProductionType"
  - [x] Select random values for each pair above (using Python).
  - [ ] Plot production/consumption as a function of time (three plots).

## Evaluation
- All students will have a random student assigned to look through their hand-ins.
  - This is an open peer review. Be polite, concise, and encouraging! Oral feedback is allowed as long as it is noted in Canvas.
- Priorities when evaluating:
  - The student completed most of the assigned tasks (remember that projects build on each other).
  - Sufficient explanations/documentation.
- Any delays (hand-in or feedback) or disagreements should be reported to the teacher.
