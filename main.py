from mylib.lib import create_spark, load_data, transform_data, query


def write_to_markdown(file_path, title, dataframe):
    """Append DataFrame content as a Markdown table in a file."""
    with open(file_path, "a") as file:
        file.write(f"## {title}\n\n")
        # Convert DataFrame rows to Markdown format
        file.write("| " + " | ".join(dataframe.columns) + " |\n")
        file.write("|" + "|".join(["---"] * len(dataframe.columns)) + "|\n")

        for row in dataframe.collect():
            file.write("| " + " | ".join([str(item) for item in row]) + " |\n")

        file.write("\n")


def main():
    # Initialize Spark Session
    spark = create_spark("RDU Weather Analysis")

    # Define the path to the output file
    output_file = "output_summary.md"

    # Path to the CSV file
    file_path = "rdu-weather-history.csv"

    # Load the dataset
    df = load_data(spark, file_path)
    write_to_markdown(output_file, "Loaded Dataset", df)

    # Transform the data
    transformed_df = transform_data(df)
    write_to_markdown(output_file, "Transformed Data", transformed_df)

    # Run SQL query on transformed data
    sql_result_df = query(transformed_df, spark)
    write_to_markdown(output_file, "Query Result", sql_result_df)

    # Stop Spark Session
    spark.stop()
    print(f"Report saved as {output_file}")


if __name__ == "__main__":
    main()
