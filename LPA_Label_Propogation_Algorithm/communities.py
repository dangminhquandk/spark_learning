import csv
from collections import Counter
from pathlib import Path


class GetComm:

    def __init__(self):
        base_dir = Path(__file__).resolve().parent
        csv_files = list((base_dir / "communities").glob("part-*.csv"))
        if not csv_files:
            raise FileNotFoundError(
                "Chưa tìm thấy file CSV trong communities/"
            )
        self.csv_path = csv_files[0]
        with open(self.csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            self.data = list(reader)

    def get_comm(self):
        counts = Counter(row["label"] for row in self.data)
        print(">> Top các cộng đồng đông nhất (theo số lượng thực thể):")
        print(f"{'Label':<18} | {'Số thực thể'}")
        print("-" * 35)
        for label, count in counts.most_common(20):
            print(f"{label:<18} | {count}")

    def get_items(self, label=None):
        if not self.data:
            return
        if label is None:
            counts = Counter(row["label"] for row in self.data)
            label = counts.most_common(1)[0][0]
        print(f"\n>> Danh sách thực thể thuộc cộng đồng (label = {label}):")
        matched = [row for row in self.data if row["label"] == str(label)]
        for i, row in enumerate(matched[:20], 1):
            print(f"{i:2d}. {row.get('url', '')}")


if __name__ == "__main__":
    gcom = GetComm()
    gcom.get_comm()
    gcom.get_items()