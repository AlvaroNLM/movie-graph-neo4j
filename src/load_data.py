import pandas as pd
from db import Neo4jConnection
import os
import config

def clean_and_parse(df):
    df["id"] = df.index
    df.rename(columns={"Series_Title": "title"}, inplace=True)
    df = df.dropna(subset=['id', 'title'])
    cols = ["id"] + [col for col in df.columns if col != "id"]
    df = df[cols]
    return df

def create_nodes_and_relationships(conn, movies_df):
    for _, movie in movies_df.iterrows():
        genres = movie['Genre'].replace("'", "").split(",")
        conn.query(
            "MERGE (m:Movie {id: $id, title: $title})",
            {"id": movie['id'], "title": movie['title']}
        )
        for genre in genres:
            conn.query(
                """
                MERGE (g:Genre {name: $genre})
                MERGE (m:Movie {id: $id})
                MERGE (m)-[:HAS_GENRE]->(g)
                """,
                {"genre": genre, "id": movie['id']}
            )

def main():
    print("Loading data...")
    # Connection 
    conn = Neo4jConnection()
    # Movies
    current_path = os.getcwd()
    movies_path = os.path.join(current_path, "data")
    movies_file = os.path.join(movies_path, config.FILE_NAME)
    movies = pd.read_csv(movies_file, low_memory=False)
    movies = clean_and_parse(movies)
    # Function
    create_nodes_and_relationships(conn, movies)
    conn.close()
    print("Data import complete.")

if __name__ == "__main__":
    main()
