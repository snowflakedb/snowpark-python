import pandas as pd
import pytz
import streamlit as st

from snowflake.snowpark import Session

AIRFLOW_REPO_LINK = "https://github.com/apache/airflow"
PROVIDER_PACKAGE_LINK = "https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/index.html"
SYSTEM_TEST_LINK = (
    "https://github.com/apache/airflow/blob/main/providers/tests/system/snowflake"
)


st.title("Airflow Snowflake System Test Dashboard")


@st.cache_data(ttl="12h")
def get_data():
    with Session.builder.configs(st.secrets.db_credentials).create() as session:
        # Query `airflow_system_test.public.results` table
        results_df = session.table("airflow_system_test.public.results").to_pandas()
        results_df.columns = results_df.columns.str.lower()

        return results_df


# Load data from Snowflake
df = get_data()

# Get the latest created_on timestamp
latest_timestamp = df["created_on"].max()

# Format the timestamp
utc_timezone = pytz.utc
pst_timezone = pytz.timezone("US/Pacific")
latest_timestamp_utc = latest_timestamp.tz_localize(utc_timezone)
latest_timestamp_pst = latest_timestamp_utc.astimezone(pst_timezone)
formatted_timestamp = latest_timestamp_pst.strftime("%Y-%m-%d %H:%M:%S %Z")

st.markdown(
    f"""
### View the health of Snowflake integrations for Apache Airflow

This live dashboard displays the current health of Snowflake integrations available in the
[Snowflake Provider package of Apache Airflow]({PROVIDER_PACKAGE_LINK}).

The following table shows data for all runs from the past 10 days of the [Snowflake System Tests]({SYSTEM_TEST_LINK})
using the latest [Apache Airflow codebase]({AIRFLOW_REPO_LINK}) (main branch).
"""
)


def process_data(df):
    # Convert 'created_on' to datetime
    df["created_on"] = pd.to_datetime(df["created_on"])

    # Calculate successes and failures
    summary_df = (
        df.groupby("dag_id")
        .agg(
            successes=pd.NamedAgg(column="success", aggfunc=lambda x: x.sum()),
            failures=pd.NamedAgg(column="success", aggfunc=lambda x: (~x).sum()),
            duration=pd.NamedAgg(column="duration", aggfunc="mean"),
        )
        .reset_index()
    )

    # Prepare the last 10 runs (latest on the right)
    last_10_runs = df.sort_values("created_on").groupby("dag_id").tail(10)

    # Create a string representation of the last 10 runs
    last_10_summary = (
        last_10_runs.groupby("dag_id")
        .apply(
            lambda x: "".join(
                ["✅" if s else "❌" for s in x.sort_values("created_on")["success"]]
            ),
        )
        .reset_index(name="last_10_runs")
    )

    # Merge the two dataframes (summary and last 10 runs)
    final_df = pd.merge(summary_df, last_10_summary, on="dag_id")

    # Add link
    final_df["dag_id"] = final_df["dag_id"].apply(
        lambda x: f"{SYSTEM_TEST_LINK}/{x}.py"
    )

    # Convert duration to a readable format: seconds if < 60, otherwise minutes
    final_df["duration"] = final_df["duration"].apply(
        lambda x: f"{int(x)} seconds" if x < 60 else f"{int(x // 60)} minutes"
    )

    # Rename columns
    final_df.columns = [
        "System Test Name",
        "Successes",
        "Failures",
        "Average Duration",
        "Last 10 Runs",
    ]

    # Format url
    final_df = final_df.style.format(
        lambda x: f'📝{x.split("/")[-1].split(".")[0]}'
        if isinstance(x, str) and x.startswith("https:")
        else x
    )

    return final_df


# Processed data
processed_data = process_data(df)

# Display the processed data as a table
st.dataframe(
    processed_data,
    hide_index=True,
    use_container_width=True,
    column_config={"System Test Name": st.column_config.LinkColumn()},
)

st.markdown(f"**Last updated: {formatted_timestamp}**")
