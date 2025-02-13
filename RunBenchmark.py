import subprocess


def execute_scripts():
    try:
        # Esegue il codice delle query di BaseX
        print("Running BaseX/Query.py script")
        subprocess.run(["python", "BaseX/Query.py"], check=True)

        # Esegue il codice delle query di Neo4j
        print("Running Neo4j/Query.py script")
        subprocess.run(["python", "Neo4j/Query.py"], check=True)

        # Esegue il codice che crea gli istogrammi in base ai risultati delle query
        print("Running Histograms/GenerateHistograms.py script")
        subprocess.run(["python", "Histograms/GenerateHistograms.py"], check=True)

    # Eccezione
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing a script: {e}")


if __name__ == "__main__":
    execute_scripts()
