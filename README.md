
import pandas as pd
import requests
import json
from typing import Dict, List, Any
import numpy as np

def extract_csv_data(csv_file_path: str = 'africa.csv') -> pd.DataFrame:
    """
    Extract data from the CSV file
    """
    try:
        df = pd.read_csv('/content/sample_data/africa.csv')
        print(f"Successfully extracted {len(df)} records from CSV file")
        return df
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file_path}' not found")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return pd.DataFrame()

def extract_api_data() -> Dict[str, Any]:
    """
    Extract data from REST Countries API
    Using REST Countries API as it provides comprehensive country data
    and doesn't require authentication
    """
    try:
        print("Extracting data from REST Countries API...")

        # Get data for multiple regions to ensure diversity
        regions = ['africa', 'asia', 'europe', 'americas', 'oceania']
        all_countries = []

        for region in regions:
            try:
                response = requests.get(f'https://restcountries.com/v3.1/region/{region}')
                if response.status_code == 200:
                    region_data = response.json()
                    all_countries.extend(region_data)
                    print(f"  - Extracted {len(region_data)} countries from {region}")
                else:
                    print(f"  - Failed to get data for {region}: {response.status_code}")
            except Exception as e:
                print(f"  - Error fetching {region} data: {e}")

        print(f"Total countries extracted from API: {len(all_countries)}")
        return all_countries

    except requests.exceptions.RequestException as e:
        print(f"Error fetching API data: {e}")
        return {}

def clean_csv_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the CSV dataset
    """
    print("Cleaning CSV data...")

    # Create a copy to avoid modifying the original
    df_cleaned = df.copy()

    # Remove duplicate country entries (South Africa appears multiple times)
    df_cleaned = df_cleaned.drop_duplicates(subset=['country'], keep='first')

    # Handle missing values
    df_cleaned = df_cleaned.dropna()

    # Standardize text fields
    df_cleaned['country'] = df_cleaned['country'].str.strip()
    df_cleaned['capital'] = df_cleaned['capital'].str.strip()
    df_cleaned['continents'] = df_cleaned['continents'].str.strip()

    # Fix any encoding issues in country names
    country_name_fixes = {
        'RÃ©union': 'Réunion',
        'SÃ£o TomÃ© and PrÃ­ncipe': 'São Tomé and Príncipe',
        'LomÃ©': 'Lomé',
        'YaoundÃ©': 'Yaoundé',
        'El AaiÃºn': 'El Aaiún'
    }

    for wrong, correct in country_name_fixes.items():
        df_cleaned['country'] = df_cleaned['country'].replace(wrong, correct)
        df_cleaned['capital'] = df_cleaned['capital'].replace(wrong, correct)

    print(f"Cleaned CSV data: {len(df_cleaned)} unique countries")
    return df_cleaned

def clean_api_data(api_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Clean and transform the API data
    """
    if not api_data:
        return pd.DataFrame()

    print("Cleaning API data...")

    cleaned_data = []

    for country in api_data:
        try:
            # Extract relevant information
            country_info = {
                'api_country_name': country.get('name', {}).get('common', ''),
                'official_name': country.get('name', {}).get('official', ''),
                'capital': country.get('capital', [''])[0] if country.get('capital') else '',
                'region': country.get('region', ''),
                'subregion': country.get('subregion', ''),
                'population': country.get('population', 0),
                'area': country.get('area', 0),
                'languages': list(country.get('languages', {}).values()) if country.get('languages') else [],
                'currencies': list(country.get('currencies', {}).keys()) if country.get('currencies') else [],
                'borders': country.get('borders', []),
                'timezones': country.get('timezones', []),
                'flag': country.get('flag', '')
            }
            cleaned_data.append(country_info)
        except Exception as e:
            print(f"Error processing country data: {e}")
            continue

    df_api = pd.DataFrame(cleaned_data)

    # Clean API data
    df_api = df_api.drop_duplicates(subset=['api_country_name'])
    df_api = df_api.dropna(subset=['api_country_name'])

    print(f"Cleaned API data: {len(df_api)} unique countries")
    return df_api

def merge_datasets(df_csv: pd.DataFrame, df_api: pd.DataFrame) -> pd.DataFrame:
    """
    Merge both datasets on country names
    """
    print("Merging datasets...")

    # Create a mapping for country name variations between datasets
    country_mapping = {
        'Ivory Coast': 'Côte d\'Ivoire',
        'DR Congo': 'Democratic Republic of the Congo',
        'Republic of the Congo': 'Congo',
        'Eswatini': 'Eswatini',
        'São Tomé and Príncipe': 'São Tomé and Príncipe',
        'Gambia': 'Gambia',
        'Cape Verde': 'Cabo Verde'
    }

    # Apply mapping to CSV data for better matching
    df_csv['country_standardized'] = df_csv['country'].map(
        lambda x: country_mapping.get(x, x)
    )

    # Try different merge strategies
    merged_df = pd.merge(
        df_csv,
        df_api,
        left_on='country_standardized',
        right_on='api_country_name',
        how='left',
        indicator=True
    )

    # Check merge results
    merge_stats = merged_df['_merge'].value_counts()
    print(f"Merge results: {merge_stats.to_dict()}")

    # Drop the temporary columns
    merged_df = merged_df.drop(['country_standardized', '_merge'], axis=1)

    print(f"Final merged dataset: {len(merged_df)} records")
    return merged_df

def store_extracted_data(api_data: Dict[str, Any], filename: str = 'extracted_data.json'):
    """
    Store extracted raw data as JSON file
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(api_data, f, indent=2, ensure_ascii=False)
        print(f"✓ Extracted data stored as: {filename}")
    except Exception as e:
        print(f"Error storing extracted data: {e}")

def store_transformed_data(df: pd.DataFrame, filename: str = 'transformed_data.csv'):
    """
    Store transformed and merged data as CSV file
    """
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"✓ Transformed data stored as: {filename}")
    except Exception as e:
        print(f"Error storing transformed data: {e}")

def perform_analysis(df: pd.DataFrame):
    """
    Perform basic analysis on the merged dataset (Bonus task)
    """
    print("\n" + "="*60)
    print("DATA ANALYSIS RESULTS")
    print("="*60)

    # Basic dataset statistics
    total_countries = len(df)
    countries_with_api_data = df['api_country_name'].notna().sum()
    match_rate = (countries_with_api_data / total_countries) * 100

    print(f"\n📊 DATASET OVERVIEW:")
    print(f"   Total countries in CSV: {total_countries}")
    print(f"   Countries successfully matched with API: {countries_with_api_data}")
    print(f"   Data match rate: {match_rate:.1f}%")

    # Continent distribution
    print(f"\n🌍 CONTINENT DISTRIBUTION:")
    continent_stats = df['continents'].value_counts()
    for continent, count in continent_stats.items():
        print(f"   {continent}: {count} countries")

    # Population analysis for matched countries
    if 'population' in df.columns and countries_with_api_data > 0:
        pop_data = df[df['population'].notna()]
        if len(pop_data) > 0:
            pop_stats = pop_data['population'].describe()

            print(f"\n👥 POPULATION ANALYSIS ({len(pop_data)} countries with data):")
            print(f"   Average population: {round(pop_stats['mean']):,}")
            print(f"   Median population: {round(pop_data['population'].median()):,}")
            print(f"   Most populous: {pop_data.loc[pop_data['population'].idxmax(), 'country']} "
                  f"({round(pop_data['population'].max()):,})")
            print(f"   Least populous: {pop_data.loc[pop_data['population'].idxmin(), 'country']} "
                  f"({round(pop_data['population'].min()):,})")

    # Regional analysis from API
    if 'region' in df.columns and countries_with_api_data > 0:
        region_data = df[df['region'].notna()]
        if len(region_data) > 0:
            print(f"\n🗺️ REGIONAL DISTRIBUTION (from API):")
            regional_dist = region_data['region'].value_counts()
            for region, count in regional_dist.items():
                print(f"   {region}: {count} countries")

    # Capital cities analysis
    capital_stats = df['capital_x'].notna().sum() # Changed 'capital' to 'capital_x'
    print(f"\n🏛️ CAPITAL CITIES:")
    print(f"   Countries with capital cities data: {capital_stats}")

    # Sample of merged data
    print(f"\n🔍 SAMPLE OF MERGED DATA (first 5 countries):")
    sample_cols = [col for col in ['country', 'capital_x', 'continents', 'region', 'population'] if col in df.columns] # Changed 'capital' to 'capital_x'
    print(df[sample_cols].head().to_string(index=False))

def main():
    """
    Main function to execute the ETL pipeline
    """
    print("🚀 STARTING ETL PIPELINE")
    print("=" * 50)

    # Extract data
    print("\n📥 STEP 1: EXTRACTING DATA...")
    csv_data = extract_csv_data('africa.csv')

    if csv_data.empty:
        print("❌ Failed to extract CSV data. Exiting pipeline.")
        return

    api_data = extract_api_data()

    if not api_data:
        print("❌ Failed to extract API data. Exiting pipeline.")
        return

    # Store extracted data
    store_extracted_data(api_data, 'extracted_data.json')

    # Transform data
    print("\n🔄 STEP 2: TRANSFORMING DATA...")
    cleaned_csv = clean_csv_data(csv_data)
    cleaned_api = clean_api_data(api_data)

    # Merge datasets
    print("\n🤝 STEP 3: MERGING DATASETS...")
    merged_data = merge_datasets(cleaned_csv, cleaned_api)

    # Store transformed data
    store_transformed_data(merged_data, 'transformed_data.csv')

    # Perform analysis (bonus)
    print("\n📈 STEP 4: PERFORMING ANALYSIS...")
    perform_analysis(merged_data)

    print("\n" + "=" * 50)
    print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 50)
    print("\n📁 FILES GENERATED:")
    print("   - extracted_data.json (raw API data)")
    print("   - transformed_data.csv (cleaned and merged data)")
    print(f"\n📊 FINAL DATASET: {len(merged_data)} countries")
    print("   Ready for analytics, reporting, and predictions!")

if __name__ == "__main__":
    main()
