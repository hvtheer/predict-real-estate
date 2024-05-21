import pandas as pd
from sklearn.preprocessing import LabelEncoder

def unique_values(df, columns):
    for column in columns:
        unique_vals = df[column].unique()
        print(f"Column '{column}' has {len(unique_vals)} unique values: {unique_vals}")
        # unique_vals_df = pd.DataFrame(unique_vals, columns=[column])
        # unique_vals_df.to_csv(f'data/results/unique_{column}.csv', index=False)
        
# Function to standardize a column using mapping
def standardize_column(column, mapping):
    if pd.isna(column):
        return 'Khác'
    
    column_lower = column.lower()
    for key in mapping:
        if key in column_lower:
            return mapping[key]
    return 'Khác'

# Standardize 'price' column
def standardize_price(row):
    price = row['price']
    if pd.isna(price):
        return None
    price = price.lower().replace(',', '.').strip()
    if 'tỷ/m²' in price:
        return round(float(price.replace(' tỷ/m²', '')) * 1e9 * row['area'])
    elif 'tỷ' in price:
        return round(float(price.replace(' tỷ', '')) * 1e9)
    elif 'triệu/m²' in price:
        return round(float(price.replace(' triệu/m²', '')) * 1e6 * row['area'])
    elif 'triệu' in price:
        return round(float(price.replace(' triệu', '')) * 1e6)
    else:
        return None
    # Write dictionaries to CSV files
def write_dictionaries_to_csv(dictionaries):
    for name, dictionary in dictionaries.items():
        df = pd.DataFrame(dictionary.items(), columns=['Value', 'Key'])
        df.to_csv(f'data/results/unique_{name}.csv', index=False)

def cleanData():
    df = pd.read_csv('data/raws/raw_data.csv')
    try:
        df.drop(['house_orientation', 'balcony_orientation'], axis=1, inplace=True)
        pass
    except:
        pass
    # Remove duplicate rows based on 'pr_id'
    df = df.drop_duplicates(subset='pr_id')
    # Remove rows where 'price' is 'Thỏa thuận'
    df = df[df['price'].str.lower() != 'thỏa thuận']
    # Columns to check
    columns_to_check = ['type_estate', 'district', 'legal_document', 'interior']
    # Get unique values and their count for the specified columns
    unique_values(df, columns_to_check)
    # Dictionary mapping for 'legal_document'
    legal_document_mapping = {
        'sổ đỏ': 'Sổ đỏ/ sổ hồng',
        'sổ hồng': 'Sổ đỏ/ sổ hồng',
        'sđcc': 'Sổ đỏ/ sổ hồng',
        'sdcc': 'Sổ đỏ/ sổ hồng',
        'hợp đồng mua bán': 'Hợp đồng mua bán',
        'hđmb': 'Hợp đồng mua bán',
        'đang chờ sổ': 'Đang chờ sổ',
        'chưa có sổ': 'Đang chờ sổ',
        'chưa sổ': 'Đang chờ sổ',
    }
    # Dictionary mapping for 'interior'
    interior_mapping = {
        'không nội thất': 'Không nội thất',
        'cơ bản': 'Cơ bản',
        'đầy đủ': 'Đầy đủ',
        'đủ đồ': 'Đầy đủ',
        'full': 'Đầy đủ',
        'cao cấp': 'Đầy đủ',
        'nội thất': 'Đầy đủ',
    }
    # Apply the standardization function to the 'legal_document' column
    df['legal_document'] = df['legal_document'].apply(lambda x: standardize_column(x, legal_document_mapping))
    # Apply the standardization function to the 'interior' column
    df['interior'] = df['interior'].apply(lambda x: standardize_column(x, interior_mapping))
    # Remove specific substrings and convert to appropriate types
    df['num_bedrooms'] = df['num_bedrooms'].str.replace(' phòng', '').astype(float)
    df['num_bathrooms'] = df['num_bathrooms'].str.replace(' phòng', '').astype(float)
    df['num_floors'] = df['num_floors'].str.replace(' tầng', '').astype(float)
    df['entrance'] = df['entrance'].str.replace(' m', '').str.replace(',', '.').astype(float)
    df['frontage'] = df['frontage'].str.replace(' m', '').str.replace(',', '.').astype(float)
    df['area'] = df['area'].str.replace(' m²', '').str.replace('.', '').str.replace(',', '.').astype(float)
    df['price'] = df.apply(standardize_price, axis=1)
    # Standardize 'posted_date' column to 'yyyy-mm-dd' format
    df['posted_date'] = pd.to_datetime(df['posted_date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    # Add 'price_per_sqm' column (price / area)
    df['price_per_sqm'] = round(df['price'] / df['area'])
    numerical_columns = ['num_bedrooms', 'num_bathrooms', 'num_floors', 'entrance', 'frontage']
    for col in numerical_columns:
        median_value = df[col].median()
        df[col].fillna(median_value, inplace=True)
    # Standardize data types for cleaned numerical fields
    df['price'] = df['price'].astype(float)
    df['area'] = df['area'].astype(float)
    df['num_bedrooms'] = df['num_bedrooms'].astype(int)
    df['num_bathrooms'] = df['num_bathrooms'].astype(int)
    df['num_floors'] = df['num_floors'].astype(int)
    df['entrance'] = df['entrance'].astype(float)
    df['frontage'] = df['frontage'].astype(float)
    df['price_per_sqm'] = df['price_per_sqm'].astype(float)
    # Normalize and encode categorical strings
    df['type_estate'] = df['type_estate'].str.strip()
    df['district'] = df['district'].str.strip()
    df['legal_document'] = df['legal_document'].str.strip()
    df['interior'] = df['interior'].str.strip()
    # Convert categorical fields to numerical codes if needed
    type_estate = {}
    district = {}
    legal_document = {}
    interior = {}
    categorical_fields = ['type_estate', 'district', 'legal_document', 'interior']
    for field in categorical_fields:
        le = LabelEncoder()
        if field == 'type_estate':
            df[field] = le.fit_transform(df[field])
            type_estate = dict(zip(le.classes_, le.transform(le.classes_)))
        elif field == 'district':
            df[field] = le.fit_transform(df[field])
            district = dict(zip(le.classes_, le.transform(le.classes_)))
        elif field == 'legal_document':
            df[field] = le.fit_transform(df[field])
            legal_document = dict(zip(le.classes_, le.transform(le.classes_)))
        elif field == 'interior':
            df[field] = le.fit_transform(df[field])
            interior = dict(zip(le.classes_, le.transform(le.classes_)))
        # df[field] = le.fit_transform(df[field])
    print(type_estate)
    print(district)
    print(legal_document)
    print(interior)

    # Call the function to write dictionaries to CSV files
    dictionaries = {
        'type_estate': type_estate,
        'district': district,
        'legal_document': legal_document,
        'interior': interior
    }
    write_dictionaries_to_csv(dictionaries)
    df.to_csv('data/results/standardized_data.csv', index=False)

cleanData()