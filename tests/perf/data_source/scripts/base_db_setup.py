#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import string
import random
import pytz
import datetime
from abc import ABC, abstractmethod


ONE_MILLION = 1_000_000
TEN_MILLION = 10_000_000
ONE_HUNDRED_MILLION = 100_000_000


class TestDBABC(ABC):
    @abstractmethod
    def __init__(self) -> None:
        self._connection = None
        pass

    def __getstate__(self):
        """Return state values to be pickled."""
        state = self.__dict__.copy()
        # Don't pickle connection
        state["_connection"] = None
        return state

    @property
    def connection(self):
        """Lazy connection creation"""
        if self._connection is None:
            self._connection = self.create_connection()
        return self._connection

    @abstractmethod
    def create_table(self):
        pass

    @staticmethod
    def generate_random_string(length=10):
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def random_datetime_with_timezone():
        # Generate a random datetime
        naive_datetime = datetime.datetime(
            2024,
            random.randint(1, 12),  # Month
            random.randint(1, 28),  # Day
            random.randint(0, 23),  # Hour
            random.randint(0, 59),  # Minute
        )
        # Assign a random timezone
        random_timezone = pytz.timezone(random.choice(pytz.all_timezones))
        timezone_aware_datetime = random_timezone.localize(naive_datetime)
        return timezone_aware_datetime

    @staticmethod
    def generate_random_data():
        raise NotImplementedError

    @abstractmethod
    def insert_data(self, num_rows=1_000_000, table_name=None):
        pass

    def _insert_data_with_sql(self, insert_sql, num_rows=1_000_000):
        full_batches = num_rows // self.insert_batch_size
        remaining_rows = num_rows % self.insert_batch_size

        with self.connection.cursor() as cursor:
            # Insert full batches
            for i in range(full_batches):
                batch_data = [
                    self.generate_random_data() for _ in range(self.insert_batch_size)
                ]
                cursor.executemany(insert_sql, batch_data)
                self.connection.commit()
                print(
                    f"Inserted batch {i + 1} with {self.insert_batch_size} rows successfully."
                )

            # Insert any remaining rows
            if remaining_rows > 0:
                batch_data = [
                    self.generate_random_data() for _ in range(remaining_rows)
                ]
                cursor.executemany(insert_sql, batch_data)
                self.connection.commit()
                print(f"Inserted final batch with {remaining_rows} rows successfully.")

            print(f"Inserted total of {num_rows} rows successfully.")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Connection closed.")
