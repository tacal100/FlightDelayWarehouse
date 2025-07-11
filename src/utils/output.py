import os


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