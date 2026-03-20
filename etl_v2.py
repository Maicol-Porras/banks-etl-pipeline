import pandas as pd 
import sqlite3
from pathlib import Path


def extract_data(csv_path):
    try:
        df = pd.read_csv(csv_path)
        print("Datos leidos correctamente.")
        return df
    except FileNotFoundError:
        print(f"Error: no se encontró el archivo {csv_path}")
        raise



def transform_data(df):
    
    # Ordenar 
    '''Ordenar de mayor a menor por Market Cap'''
    df_sorted = df.sort_values(by="Market_Cap_Billion_USD", ascending=False)
    print("\nBancos ordenados de mayor a menor por Market Cap:")
    print(df_sorted.head())

    # Filtrar
    '''Filtrar bancos con Market Cap mayor a 150'''
    df_filtered = df[df["Market_Cap_Billion_USD"] > 150] 
    print("\nBancos con Market Cap mayor a 150:")
    print(df_filtered)

    # Seleccionar columnas 
    '''Seleccionar solo ciertas columnas'''
    df_selected = df[["Bank", "Country"]]
    print("\nSolo nombres del banco y pais:")
    print(df_selected)

    # Agrupar
    '''Agrupar por pais y contar cuantos bancos hay'''
    df_grouped = df.groupby("Country").size() 
    print("\nCantidad de bancos por pais:")
    print(df_grouped)

    # Promedio por pais 
    df_avg = df.groupby("Country", as_index=False)["Market_Cap_Billion_USD"].mean()
    print("\nPromedio de Market Cap por pais:")
    print(df_avg)

    return df_sorted, df_filtered, df_selected, df_grouped, df_avg



def load_to_csv(df_avg, output_path):
    df_avg.to_csv(output_path, index=False)
    print("Archivo CSV guardado correctamente.")



def load_to_db(df, db_path):
    conn = sqlite3.connect(db_path)
    df.to_sql("banks", conn, if_exists='replace', index=False)
    print("Tabla 'banks' cargada en SQLite correctamente.")
    
    return conn



def run_queries(conn):
    cursor = conn.cursor()
    
    print("\n--- Todos los bancos en la tabla SQL ---")
    query_1 = "SELECT * FROM banks"
    for row in cursor.execute(query_1):
        print(row)

    print("\n--- Tabla 'Bank' y 'Country' ---")
    query_2 = "SELECT Bank, Country FROM banks"
    for row in cursor.execute(query_2):
        print(row)

    print("\n--- Bancos con Market Cap mayor a 150 ---")
    query_3 = """
    SELECT Bank, Country, Market_Cap_Billion_USD
    FROM banks
    WHERE Market_Cap_Billion_USD > 150
    """
    for row in cursor.execute(query_3):
        print(row)

    print("\n--- Bancos ordenados de mayor a menor por Market Cap ---")
    query_4 = """
    SELECT Bank, Country, Market_Cap_Billion_USD
    FROM banks
    ORDER BY Market_Cap_Billion_USD DESC
    """
    for row in cursor.execute(query_4):
        print(row)

    print("\n--- Cantidad de bancos por pais ---")
    query_5 = """
    SELECT Country, COUNT(*)
    FROM banks
    GROUP BY Country
    """
    for row in cursor.execute(query_5):
        print(row)



def main():
    csv_path = "data/banks.csv"
    output_csv = "data/banks_avg_by_country.csv"
    db_path = "data/banks.db"

    df = extract_data(csv_path)
    df_sorted, df_filtered, df_selected, df_grouped, df_avg = transform_data(df)
    load_to_csv(df_avg, output_csv)

    with sqlite3.connect(db_path) as conn:  
        df.to_sql("banks", conn, if_exists='replace', index=False)
        run_queries(conn)



if __name__=="__main__":
    main()