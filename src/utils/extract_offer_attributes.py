from typing import Any, Dict, List


def extract_offer_attribute_rows(raw_offer: Dict[str, Any], game_name: str) -> List[Dict[str, Any]]:
    rows = []

    offer_id = raw_offer.get("offer_id")
    title = raw_offer.get("title")
    attributes = raw_offer.get("offer_attributes", [])

    for attr in attributes:
        rows.append(
            {
                "game_name": game_name,
                "offer_id": offer_id,
                "title": title,
                "collection_id": attr.get("collection_id"),
                "dataset_id": attr.get("dataset_id"),
            }
        )

    return rows


def build_offer_attribute_dict(raw_offer: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "offer_id": raw_offer.get("offer_id"),
        "title": raw_offer.get("title"),
        "attributes": {
            attr.get("collection_id"): attr.get("dataset_id")
            for attr in raw_offer.get("offer_attributes", [])
        },
    }