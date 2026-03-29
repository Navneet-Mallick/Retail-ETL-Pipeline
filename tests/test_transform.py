import unittest
import pandas as pd

from transform import age_group, clean_transform_with_report, create_aggregations


class TestTransformFunctions(unittest.TestCase):
    def test_age_group_buckets(self):
        self.assertEqual(age_group(20), "18-25")
        self.assertEqual(age_group(30), "26-35")
        self.assertEqual(age_group(40), "36-45")
        self.assertEqual(age_group(50), "46-60")
        self.assertEqual(age_group(61), "60+")

    def test_clean_transform_with_report(self):
        raw = pd.DataFrame(
            [
                {
                    "Transaction ID": 1,
                    "Date": "2024-01-01",
                    "Customer ID": "C1",
                    "Gender": "male",
                    "Age": 25,
                    "Product Category": "electronics",
                    "Quantity": 2,
                    "Price per Unit": 100,
                    "Total Amount": 1,
                },
                {
                    "Transaction ID": 1,
                    "Date": "2024-01-01",
                    "Customer ID": "C1",
                    "Gender": "male",
                    "Age": 25,
                    "Product Category": "electronics",
                    "Quantity": 2,
                    "Price per Unit": 100,
                    "Total Amount": 1,
                },
                {
                    "Transaction ID": 2,
                    "Date": "bad-date",
                    "Customer ID": "C2",
                    "Gender": "female",
                    "Age": 30,
                    "Product Category": "beauty",
                    "Quantity": 1,
                    "Price per Unit": 50,
                    "Total Amount": 50,
                },
                {
                    "Transaction ID": 3,
                    "Date": "2024-01-02",
                    "Customer ID": "C3",
                    "Gender": "female",
                    "Age": -1,
                    "Product Category": "clothing",
                    "Quantity": 1,
                    "Price per Unit": 20,
                    "Total Amount": 20,
                },
            ]
        )

        clean_df, report = clean_transform_with_report(raw)
        self.assertEqual(len(clean_df), 1)
        self.assertIn("Age Group", clean_df.columns)
        self.assertIn("Month", clean_df.columns)
        self.assertIn("Year", clean_df.columns)
        self.assertEqual(float(clean_df.iloc[0]["Total Amount"]), 200.0)
        self.assertEqual(report["duplicate_rows_dropped"], 1)
        # bad date row is coerced to NaT then caught by null drop (type conversion runs first)
        self.assertEqual(report["rows_dropped_null_required"], 1)
        self.assertEqual(report["rows_dropped_business_rules"], 1)
        self.assertEqual(report["rows_output_clean"], 1)

    def test_create_aggregations(self):
        df = pd.DataFrame(
            [
                {
                    "Date": pd.Timestamp("2024-01-01"),
                    "Product Category": "Electronics",
                    "Quantity": 2,
                    "Gender": "Male",
                    "Total Amount": 200.0,
                },
                {
                    "Date": pd.Timestamp("2024-01-01"),
                    "Product Category": "Beauty",
                    "Quantity": 1,
                    "Gender": "Female",
                    "Total Amount": 50.0,
                },
            ]
        )
        summary = create_aggregations(df)
        self.assertFalse(summary.empty)
        self.assertIn("metric", summary.columns)
        self.assertIn("dimension", summary.columns)
        self.assertIn("value", summary.columns)


if __name__ == "__main__":
    unittest.main()
