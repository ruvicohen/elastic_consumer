from app.db.database_elastic import elastic_client


def setup_index():
    index_name = "terror_events"

    if not elastic_client.indices.exists(index=index_name):
        elastic_client.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "date": {
                            "properties": {
                                "year": {"type": "integer"},
                                "month": {"type": "integer"},
                                "day": {"type": "integer"},
                            }
                        },
                        "location": {
                            "properties": {
                                "region": {"type": "text"},
                                "country": {"type": "text"},
                                "city": {"type": "text"},
                                "latitude": {"type": "float"},
                                "longitude": {"type": "float"},
                            }
                        },
                        "groups_involved": {"type": "text"},
                        "attack_type": {"type": "text"},
                        "target_type": {"type": "text"},
                        "casualties": {
                            "properties": {
                                "fatalities": {"type": "integer"},
                                "injuries": {"type": "integer"},
                                "score": {"type": "integer"},
                            }
                        },
                        "description": {"type": "text"},
                    }
                }
            },
        )


def insert_terror_event(terror_event):
    index_name = "terror_events"
    try:
        response = elastic_client.index(index=index_name, document=terror_event)
        print(f"Document inserted with ID: {response['_id']}")
    except Exception as e:
        print(f"Failed to insert document: {e}")


def search_text_in_all_fields(text):
    index_name = "terror_events"

    response = elastic_client.search(
        index=index_name, body={"query": {"query_string": {"query": text}}}
    )
    return response["hits"]["hits"]


def delete_all_documents():
    index_name = "terror_events"
    try:
        response = elastic_client.delete_by_query(
            index=index_name, body={"query": {"match_all": {}}}
        )
        print(f"Deleted {response['deleted']} documents from index '{index_name}'.")
        return response
    except Exception as e:
        print(f"Failed to delete all documents from index '{index_name}': {e}")
        return None


def get_all_documents():
    index_name = "terror_events"
    try:
        response = elastic_client.search(
            index=index_name, body={"query": {"match_all": {}}}, size=100000
        )
        hits = response.get("hits", {}).get("hits", [])
        return hits
    except Exception as e:
        print(f"Failed to retrieve documents: {e}")
        return []


def get_document_count():
    index_name = "terror_events"
    try:
        response = elastic_client.count(
            index=index_name, body={"query": {"match_all": {}}}
        )
        return response.get("count", 0)
    except Exception as e:
        print(f"Failed to retrieve document count: {e}")
        return 0


print(get_document_count())
