def extract_data_from_csv(file_path):
    import pandas as pd
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def extract_data_from_excel(file_path):
    import pandas as pd
    try:
        data = pd.read_excel(file_path)
        return data
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None

def extract_data(file_path):
    if file_path.endswith('.csv'):
        return extract_data_from_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return extract_data_from_excel(file_path)
    else:
        print("Unsupported file format")
        return None