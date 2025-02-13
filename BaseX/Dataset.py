import pandas as pd

from BaseXClient import BaseXClient

# Nome dei file CSV da cui leggere i dati delle entità
csv_files = {
    "administrators": "Dataset/File/administrators.csv",
    "companies": "Dataset/File/companies.csv",
    "kyc_aml_checks": "Dataset/File/kyc_aml_checks.csv",
    "shareholders": "Dataset/File/shareholders.csv",
    "transactions": "Dataset/File/transactions.csv",
    "ubo": "Dataset/File/ubo.csv",
}

# Legge tutti i file CSV in un unico DataFrame pandas, aggiungendo una colonna 'entity_type' per indicare l'entità di origine
dfs = []
for entity_type, file_path in csv_files.items():
    df = pd.read_csv(file_path, encoding="ISO-8859-1")
    df["entity_type"] = entity_type
    dfs.append(df)

df = pd.concat(dfs, ignore_index=True)  # Combina tutti i DataFrame in uno solo


# Funzione per convertire un DataFrame in XML con escaping corretto dei caratteri speciali
def escape_xml_chars(text):
    if isinstance(text, str):
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;")
        )
    return text


# Funzione per convertire un DataFrame in XML, omettendo i campi non presenti o NaN e aggiungendo l'attributo entity_type
def dataframe_to_xml(df):
    xml = ["<ubo_records>"]
    for _, row in df.iterrows():
        entity_type = row["entity_type"]
        xml.append(
            f'  <ubo_record entity_type="{entity_type}">'
        )  # Aggiunge attributo entity_type
        for field in df.columns:
            if field != "entity_type":  # Non aggiunge il campo 'entity_type' come tag
                value = row[field]
                if pd.notna(value):  # Aggiunge solo i campi non NaN
                    escaped_value = escape_xml_chars(
                        str(value)
                    )  # Escapa i caratteri speciali
                    xml.append(f"    <{field}>{escaped_value}</{field}>")
        xml.append("  </ubo_record>")
    xml.append("</ubo_records>")
    return "\n".join(xml)


# Funzione per connettersi a BaseX e inserire i dati
def insert_into_basex(db_name, xml_data):
    try:
        # Come da Repository ufficiale di BaseX per connettersi al db
        print("Trying to connect to BaseX Server...")
        session = BaseXClient.Session("localhost", 1984, "admin", "admin")
        print("Connected to BaseX!")
        try:
            print(f"Creating database: {db_name}")
            session.execute(f"CREATE DB {db_name}")
            print(f"Length of XML data for {db_name}: {len(xml_data)} characters")
            session.add(f"{db_name}.xml", xml_data)
            print(f"Data successfully loaded into {db_name} in BaseX.")
        except Exception as e:
            print(f"An error occurred during data insertion: {e}")
        finally:
            session.close()
    except Exception as e:
        print(f"Connection error: {e}")


# Crea il database 100%
def create_db_100(df):
    data_100_xml = dataframe_to_xml(df)
    insert_into_basex("UBO_100", data_100_xml)


# Crea il database 75% dal 100%
def create_db_75(df):
    df_75 = df.sample(
        frac=0.75, random_state=1
    )  # Campiona il 75% del dataset originale
    data_75_xml = dataframe_to_xml(df_75)
    insert_into_basex("UBO_75", data_75_xml)
    return df_75  # Restituisce il dataframe 75%


# Crea il database 50% dal 75%
def create_db_50(df_75):
    df_50 = df_75.sample(
        frac=0.6667, random_state=1
    )  # 50% di 75% = 66.67% del totale originale
    data_50_xml = dataframe_to_xml(df_50)
    insert_into_basex("UBO_50", data_50_xml)
    return df_50  # Restituisce il dataframe 50%


# Crea il database 25% dal 50%
def create_db_25(df_50):
    df_25 = df_50.sample(
        frac=0.5, random_state=1
    )  # 50% di 50% = 50% del totale originale
    data_25_xml = dataframe_to_xml(df_25)
    insert_into_basex("UBO_25", data_25_xml)


# Avvia il processo di creazione sequenziale dei database
df_100 = df  # Dataset originale
create_db_100(df_100)  # Crea il database 100%
df_75 = create_db_75(df_100)  # Crea il database 75%
df_50 = create_db_50(df_75)  # Crea il database 50%
create_db_25(df_50)  # Crea il database 25%
