# Example 1

Here I start with a small dataframe of holidays in the US. I do some transformations on the dataset, and then iterate through its rows. Customers who want to migrate pandas workflows to Snowflake often start with some iterative code.

Each iteration requires executing one or more SQL queries on Snowflake and materializing the result. This cell ends up taking over 2 minutes, so I'm going to stop it for now.


Now I will enable hybrid execution.

I create the dataframe in the same way, and it's still a Snowpark pandas dataframe, but I can see that its backend is "pandas."

Now iterating through the dataframe is much faster.

# Example 2

Now I'll show how we decide what to do with data that we read from Snowflake.

I start by reading an entire table from Snowflake.

It has 10 million rows, so Snowpark pandas does not pull it all into memory.

I can do some transformations on the data, then aggregate it, and print the results. The computation happens in Snowflake, and scales well with the number of rows.

Now I read a subset of the table by filtering with my SQL query.

Snowpark pandas recognizes that I'm working with much less data, so it moves the result to pandas.

# Example 3

Now suppose we start with two dataframes from above, each on a different backend. We can join the two dataframes together, and Snowpark pandas will automatically move the smaller dataset to Snowflake.



# Timing

2 minutes with script
