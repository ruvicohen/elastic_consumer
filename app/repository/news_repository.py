import json

from app.db.database_elastic import elastic_client


def setup_index_for_json():
    index_name = "news_events"
    if not elastic_client.indices.exists(index=index_name):
        elastic_client.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "body": {"type": "text"},
                        "date": {"type": "date"},
                        "classification": {"type": "text"},
                        "city": {"type": "text"},
                        "country": {"type": "text"},
                        "region": {"type": "text"},
                        "latitude": {"type": "float"},
                        "longitude": {"type": "float"}
                    }
                }
            }
        )
        print(f"Index '{index_name}' created.")
    else:
        print(f"Index '{index_name}' already exists.")

def is_document_similar(news_document, index_name="news_events"):
    query = {
        "bool": {
            "must": [
                {"match": {"title": news_document["title"]}},
                {"match": {"body": news_document["body"]}}
            ]
        }
    }

    try:
        search_response = elastic_client.search(index=index_name, body={"query": query})
        hits = search_response.get("hits", {}).get("hits", [])
        return len(hits) > 0
    except Exception as e:
        print(f"Error checking for similar document: {e}")
        return False

def insert_news_document(news_document):
    index_name = "news_events"

    try:
        response = elastic_client.index(index=index_name, document=news_document)
        print(f"Document inserted with ID: {response['_id']}")
    except Exception as e:
        print(f"Failed to insert document: {e}")

def insert_news_document_if_unique(news_document):
    if is_document_similar(news_document):
        print("Similar document already exists. Skipping insertion.")
        return False

    return insert_news_document(news_document)

def get_all_documents():
    index_name = "news_events"
    try:
        response = elastic_client.search(index=index_name, body={"query": {"match_all": {}}}, size=10000)
        hits = response.get("hits", {}).get("hits", [])
        print(f"Found {len(hits)} documents:")
        for hit in hits:
            print(hit["_source"])
        return hits
    except Exception as e:
        print(f"Failed to retrieve documents: {e}")
        return []

def search_news_documents(query):
    index_name = "news_events"
    try:
        response = elastic_client.search(index=index_name, body={"query": query})
        hits = response.get("hits", {}).get("hits", [])
        print(f"Found {len(hits)} documents:")
        for hit in hits:
            print(hit["_source"])
        return hits
    except Exception as e:
        print(f"Failed to search documents: {e}")
        return []

def delete_all_documents():
    index_name = "news_events"
    try:
        response = elastic_client.delete_by_query(
            index=index_name,
            body={"query": {"match_all": {}}}
        )
        print(f"Deleted {response['deleted']} documents from index '{index_name}'.")
        return response
    except Exception as e:
        print(f"Failed to delete all documents from index '{index_name}': {e}")
        return None