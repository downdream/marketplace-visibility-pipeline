import matplotlib.pyplot as plt
import pandas as pd

from src.database.db_select import read_table


def load_backmarket_data() -> pd.DataFrame:
    return read_table("Backmarket", "Backmarket_va")


def filter_offer_data(data: pd.DataFrame, offer_id: str) -> pd.DataFrame:
    product_df = data[data["offer_id"] == offer_id].copy()
    product_df["date"] = pd.to_datetime(product_df["date"], format="%Y-%m-%d")
    product_df = product_df.sort_values("date")
    return product_df


def create_rank_plot(product_df: pd.DataFrame, offer_id: str) -> None:
    plt.figure(figsize=(12, 6))
    plt.plot(product_df["date"], product_df["rank"])
    plt.xlabel("Date")
    plt.ylabel("Rank")
    plt.title(f"Rank Changes for Offer ID: {offer_id}")
    plt.grid(True)
    plt.gca().invert_yaxis()  # rank 1 should appear higher than rank 10
    plt.show()


def main(offer_id: str) -> None:
    data = load_backmarket_data()
    product_df = filter_offer_data(data, offer_id)

    if product_df.empty:
        print(f"No data found for offer_id: {offer_id}")
        return

    create_rank_plot(product_df, offer_id)


if __name__ == "__main__":
    offer_id = "f494a8a4-ef58-4a1c-9495-a64d21fed02f"
    main(offer_id)