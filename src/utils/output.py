import os
import matplotlib.pyplot as plt
import seaborn as sns

def print_tables(tables):
    """Print all normalized tables"""
    for table_name, table_data in tables.items():
        if not table_data.empty:
            print(f"\n{'='*50}")
            print(f"{table_name.upper()} TABLE ({len(table_data)} rows)")
            print(f"{'='*50}")
            print(table_data.head(10).to_string(index=False))
            print(f"Columns: {list(table_data.columns)}")
        else:
            print(f"\n{table_name.upper()} TABLE: No data available")

def save_tables(tables,suffix, output_dir="./Data/tables"):
    """Save normalized tables to CSV files"""
    os.makedirs(output_dir, exist_ok=True)
    
    for table_name, table_data in tables.items():
        if not table_data.empty:
            output_path = os.path.join(output_dir, f"{table_name}{suffix}_table.csv")
            table_data.to_csv(output_path, index=False)
            print(f"Saved {table_name}{suffix} table to: {output_path}")



def plot_flight_data_eda(df):
    """Generate EDA plots to find outliers in flight data"""
    plt.figure(figsize=(12, 5))
    # 1. Departure Delay Distribution
    plt.subplot(1, 2, 1)
    sns.histplot(df['DEP_DELAY'].dropna(), bins=100, kde=True)
    plt.title('Departure Delay Distribution')
    plt.xlabel('Departure Delay (minutes)')
    plt.ylabel('Count')
    
    # 2. Temperature Distribution
    plt.subplot(1, 2, 2)
    sns.histplot(df['TEMPERATURE'].dropna(), bins=50, kde=True, color='orange')
    plt.title('Temperature Distribution (°C)')
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Count')
    
    plt.tight_layout()
    plt.show()

    # 3. Boxplot for Departure Delay by Cancellation Status
    plt.figure(figsize=(7, 5))
    sns.boxplot(x='CANCELLED', y='DEP_DELAY', data=df)
    plt.title('Departure Delay by Cancellation Status')
    plt.xlabel('Cancelled')
    plt.ylabel('Departure Delay (minutes)')
    plt.show()

    # 5. Outlier Detection: Wind Speed
    plt.figure(figsize=(7, 5))
    sns.boxplot(x=df['WIND_SPD'].dropna())
    plt.title('Wind Speed Outliers')
    plt.xlabel('Wind Speed (knots)')
    plt.show()
