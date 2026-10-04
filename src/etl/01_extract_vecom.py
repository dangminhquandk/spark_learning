"""
Tầng ETL: Trích xuất và làm sạch dữ liệu Báo cáo VECOM 2025.
Đầu vào: data/raw/vecom_2025.csv (giả định)
Đầu ra: 
- data/silver/vertices.parquet (Danh sách thực thể: Tỉnh thành, Doanh nghiệp)
- data/silver/edges.parquet (Danh sách mối quan hệ kinh tế)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def extract_entities_and_relations(spark: SparkSession):
    print("ETL Job: Đang xử lý dữ liệu để tạo Knowledge Graph Vertices và Edges...")
    # Sẽ implement trong tương lai
    pass

if __name__ == "__main__":
    pass
