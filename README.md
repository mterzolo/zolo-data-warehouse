# zolo-data-warehouse
Scripts for creating and maintaining a data warehouse for a coffee roasting company

1) Connect to APIs to pull in data
  * Square Transactions
  * Quickbooks
  * Shopify
  * Cropster
  
2) Transform data

3) Load to database

4) Hook up self service analytics tools and create reports


## Projects that will use this data

### Quittin Time
Continuously monitor sales at the farmer's market and forecast out sales in the next 30 mins. If the predicted sales are lower than the variable costs of running the tent, then it would be prudent to shut down the tent for the day.

### Weekly Roast Schedule
Predict demand of roasted coffee for the week to reduce spoilage (When we roast too much coffee) and out-of-stock (When we don't roast enough coffee. Once the database is connecting we will understand our baseline spoilage / out-of-stock rates and seek to lower them using statistical models.

### How much Cash in the Stash
Each farmers' market needs cash in the register to return change to customers. Look at historical data to minimize how much cash we need to put in the register, while ensuring that we don't run out of any small bills throughout the day.
