import subprocess


def execute_scripts():
    try:
        # Esegue il codice per generare automaticamente i dataset
        print("Running Dataset/GenerateDataset.py script")
        subprocess.run(["python", "Dataset/GenerateDataset.py"], check=True)

        # Esegue il codice per inserire i dati all'interno di BaseX
        print("Running BaseX/Dataset.py script")
        subprocess.run(["python", "BaseX/Dataset.py"], check=True)

        # Esegue il codice per inserire i dati all'interno di Neo4j
        print("Running Neo4j/Dataset.py script")
        subprocess.run(["python", "Neo4j/Dataset.py"], check=True)

    # Eccezione
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing a script: {e}")


if __name__ == "__main__":
    execute_scripts()
