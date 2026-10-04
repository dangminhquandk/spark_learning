"""
Tầng Graph Engine: Khai phá Đồ thị Tri thức Kinh tế (Economic Knowledge Graph)
Phân tích thuật toán trên đồ thị:
- PageRank: Xác định địa phương trọng điểm (Economic Hubs) dựa trên luồng giao thương.
- LPA (Label Propagation): Tự động phân cụm các vùng kinh tế liên kết mật thiết.
"""
import os
import sys
from pathlib import Path

# Thêm thư mục gốc vào đường dẫn hệ thống
BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))

# Đảm bảo môi trường thực thi chuẩn theo quy tắc dự án
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from graphframes import GraphFrame

from src.utils.profiler import profile_block


def create_spark_session() -> SparkSession:
    """Khởi tạo SparkSession chuẩn cho GraphFrames trên Apple Silicon (Mac M3)."""
    spark = (
        SparkSession.builder
        .appName("VECOM_Economic_Knowledge_Graph")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.13")
        .getOrCreate()
    )
    # Checkpoint là bắt buộc để cắt tỉa RDD Lineage trong các giải thuật lặp (PageRank, LPA)
    checkpoint_dir = str(BASE_DIR / ".spark_checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    return spark


def build_economic_network(spark: SparkSession) -> GraphFrame:
    """
    BƯỚC 1: Dựng mạng lưới giao thương các tỉnh thành Việt Nam.
    - Vertices (Đỉnh): Tỉnh/Thành phố (Bắt buộc có cột 'id').
    - Edges (Cạnh): Dòng giao dịch TMĐT (Bắt buộc có cột 'src' và 'dst').
    """
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

    # Luồng giao thương: src -> dst với sản lượng thương mại (tỷ VNĐ)
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

    return GraphFrame(vertices, edges)


def analyze_economic_hubs(g: GraphFrame, max_iter: int = 10, reset_prob: float = 0.15):
    """
    BƯỚC 2: Thuật toán PageRank xác định các cực tăng trưởng (Economic Hubs).
    - resetProbability = 0.15 tương ứng damping factor = 0.85.
    - Địa phương nhận dòng tiền lớn từ các nút có bậc cao sẽ có PageRank cao.
    """
    pr_result = g.pageRank(resetProbability=reset_prob, maxIter=max_iter)
    return pr_result.vertices.select("id", "name", "region", "pagerank").orderBy(
        F.col("pagerank").desc()
    )


def analyze_economic_clusters(g: GraphFrame, max_iter: int = 5):
    """
    BƯỚC 3: Thuật toán Label Propagation (LPA) phát hiện cụm cộng đồng giao thương.
    - Tìm ra các nhóm tỉnh thành liên kết hữu cơ nội vùng.
    """
    lpa_result = g.labelPropagation(maxIter=max_iter)
    return lpa_result.select("id", "name", "region", "label").orderBy("label", "region")


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    with profile_block("Khởi tạo Economic Knowledge Graph"):
        g = build_economic_network(spark)
        print(">> Danh sách các Tỉnh/Thành (Vertices):")
        g.vertices.show(truncate=False)
        print(">> Luồng giao thương TMĐT (Edges):")
        g.edges.show(truncate=False)

    with profile_block("Phân tích PageRank (Economic Hubs)"):
        print("\n🏆 [PAGERANK] BẢNG XẾP HẠNG CỰC TĂNG TRƯỞNG KINH TẾ:")
        hubs_df = analyze_economic_hubs(g, max_iter=10)
        hubs_df.show(truncate=False)

    with profile_block("Phân tích LPA (Economic Communities)"):
        print("\n🌐 [LPA] PHÂN CỤM CỘNG ĐỒNG GIAO THƯƠNG NỘI VÙNG:")
        clusters_df = analyze_economic_clusters(g, max_iter=5)
        clusters_df.show(truncate=False)

    spark.stop()
