import sqlite3
import streamlit as st

# Function to fetch and process data
def fetch_data():
    # Connect to the SQLite database
    conn = sqlite3.connect('websocket_data.db')
    cursor = conn.cursor()

    # Execute a query to fetch the data
    cursor.execute('''
        SELECT id, price
        FROM ticker_data
        ORDER BY id
    ''')

    # Fetch all results
    results = cursor.fetchall()

    # Close the database connection
    conn.close()

    return results

# Main function to display data using Streamlit
def main():
    st.title('batch mode analysis')

    # Fetch data
    results = fetch_data()

    # Number of rows and columns
    num_rows = len(results) - 4  # Because of the sliding window
    num_cols = 4  # Number of columns (Serial No, Min Price, Max Price, Avg Price)

    # Create columns for each component
    cols = st.columns(num_cols)

    # Print the header
    header = ["Serial No", "Min Price", "Max Price", "Avg Price"]
    for i, col in enumerate(cols):
        col.write(header[i])

    # Iterate through the rows with sliding window
    window_size = 5
    for i in range(num_rows):
        window_data = results[i:i+window_size]
        serial_no = f"{window_data[0][0]} to {window_data[-1][0]}"
        min_price = min(row[1] for row in window_data)
        max_price = max(row[1] for row in window_data)
        avg_price = sum(row[1] for row in window_data) / window_size

        # Write data to respective columns
        cols[0].write(serial_no)
        cols[1].write(min_price)
        cols[2].write(max_price)
        cols[3].write(avg_price)

# Call the main function
if __name__ == "__main__":
    main()

