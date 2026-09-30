"""
Tầng Graph Engine: Khai phá Đồ thị Tri thức Kinh tế (Economic Knowledge Graph)
Phân tích thuật toán trên đồ thị:
- PageRank: Tìm địa phương trọng điểm (Hub).
- LPA: Phân cụm các vùng kinh tế lân cận.
"""
import sys
from pathlib import Path

# Thêm thư mục gốc vào đường dẫn hệ thống
sys.path.append(str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession
from graphframes import GraphFrame

def create_spark_session():
    """Khởi tạo SparkSession chuẩn cho GraphFrames trên Mac M3."""
    return (
        SparkSession.builder
        .appName("VECOM_Economic_Knowledge_Graph")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def build_economic_network(spark):
    """
    BƯỚC 1: Dựng mạng lưới giao thương các tỉnh thành Việt Nam.
    - Vertices (Đỉnh): Tỉnh/Thành phố (Bắt buộc có cột 'id').
    - Edges (Cạnh): Dòng giao dịch TMĐT (Bắt buộc có cột 'src' và 'dst').
    """
    # 1. Bảng Đỉnh: Các trung tâm kinh tế đại diện
    provinces_data = [
        ("HN", "Ha Noi", "Mien Bac"),
        ("BN", "Bac Ninh", "Mien Bac"),
        ("HP", "Hai Phong", "Mien Bac"),
        ("DN", "Da Nang", "Mien Trung"),
        ("HUE", "Thua Thien Hue", "Mien Trung"),
        ("HCM", "TP. Ho Chi Minh", "Mien Nam"),
        ("BD", "Binh Duong", "Mien Nam"),
        ("CT", "Can Tho", "Mien Nam")
    ]
    vertices = spark.createDataFrame(provinces_data, ["id", "name", "region"])

    # 2. Bảng Cạnh: Luồng giao thương (src -> dst với sản lượng thương mại 'volume' tỷ VNĐ)
    trade_flows = [
        # Cụm kinh tế phía Bắc (giao thương nội bộ mạnh)
        ("BN", "HN", 120.5),
        ("HP", "HN", 95.0),
        ("HN", "HP", 60.0),
        # Cụm kinh tế phía Nam (giao thương nội bộ mạnh)
        ("BD", "HCM", 250.0),
        ("CT", "HCM", 110.0),
        ("HCM", "BD", 180.0),
        # Trục kết nối Bắc - Nam (qua hai đầu tàu HN và HCM)
        ("HN", "HCM", 310.0),
        ("HCM", "HN", 290.0),
        # Trục kết nối Miền Trung
        ("DN", "HN", 75.0),
        ("DN", "HCM", 85.0),
        ("HUE", "DN", 30.0)
    ]
    edges = spark.createDataFrame(trade_flows, ["src", "dst", "trade_volume"])

    # 3. Đóng gói vào GraphFrame
    g = GraphFrame(vertices, edges)
    return g

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- [1] KHỞI TẠO ECONOMIC KNOWLEDGE GRAPH ---")
    g = build_economic_network(spark)

    print(">> Danh sách các Tỉnh/Thành (Vertices):")
    g.vertices.show()

    print(">> Luồng giao thương TMĐT (Edges):")
    g.edges.show()

    spark.stop()
