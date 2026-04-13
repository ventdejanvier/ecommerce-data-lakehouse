import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
sys.path.append('/home/jovyan/scripts')
from common_config import get_spark_session

def transform_gold():
    # Khởi tạo Spark Session cho tầng Gold
    spark = get_spark_session("Gold_Layer")
    
    # Load data from Silver layer
    print("Loading data from Silver...")
    df_silver = spark.read.format("delta").load("s3a://silver/electronics_events")

    #PHÂN TÍCH GIÁ (Lấy giá trung bình từng sản phẩm để đưa vào Dim)
    product_price_ref = df_silver.groupBy("product_id").agg(
        F.round(F.avg("price"), 2).alias("avg_price")
    )
 
    # dim_products sẽ là bảng dimension chính cho sản phẩm, chứa thông tin hiển thị và phân loại
    # ---------------------------------------------------------
    split_col = F.split(df_silver['category_fixed'], '\.')
    
    dim_products = df_silver.select("product_id", "brand_fixed", "category_fixed").distinct() \
        .join(product_price_ref, on="product_id", how="left") \
        .withColumn("brand", 
            F.when(F.col("brand_fixed") == "Generic_Brand", "No Brand")
             .otherwise(F.initcap(F.col("brand_fixed")))) \
        .withColumn("cat_l1_raw", split_col.getItem(0)) \
        .withColumn("category_main", 
            F.when(F.col("cat_l1_raw").contains("Category_"), "Uncategorized")
             .otherwise(F.initcap(F.col("cat_l1_raw")))) \
        .withColumn("category_sub", F.initcap(split_col.getItem(1))) \
        .withColumn("category_detail", F.initcap(split_col.getItem(2))) \
        .withColumn("display_name", 
            F.concat(F.col("brand"), F.lit(" "), 
                     F.regexp_replace(F.col("category_fixed"), "\.", " "), 
                     F.lit(" - #"), F.col("product_id").cast("string"))) \
        .select(
            "product_id", "display_name", "brand", "avg_price",
            "category_main", "category_sub", "category_detail"
        )
 
    # user_interactions sẽ là bảng fact chứa điểm tương tác giữa user và sản phẩm, dùng cho recommendation engine
    max_date = df_silver.select(F.max("event_time")).collect()[0][0]
    interaction_matrix = df_silver.groupBy("user_id", "product_id").agg(
        F.sum(F.when(F.col("event_type") == "view", 1)
              .when(F.col("event_type") == "cart", 3)
              .when(F.col("event_type") == "purchase", 5)
              .otherwise(0)).alias("base_weight"),
        F.count("event_type").alias("interaction_count"),
        F.min(F.datediff(F.lit(max_date), F.col("event_time"))).alias("days_since_last_interact")
    ).withColumn("interaction_score", 
        F.round((F.col("base_weight") * F.log1p(F.col("interaction_count"))) / 
                (F.lit(1) + F.lit(0.01) * F.col("days_since_last_interact")), 4)) \
    .select("user_id", "product_id", "interaction_score")
 
    # funnel_analysis sẽ là bảng tổng hợp số liệu để phân tích hành vi người dùng qua các bước của phễu mua hàng
    funnel_analysis = df_silver.groupBy("category_fixed").agg(
        F.count(F.when(F.col("event_type") == "view", True)).alias("total_views"),
        F.count(F.when(F.col("event_type") == "cart", True)).alias("total_carts"),
        F.count(F.when(F.col("event_type") == "purchase", True)).alias("total_purchases")
    ).withColumn("view_to_cart_rate", F.round(F.col("total_carts") / F.col("total_views"), 4)) \
     .withColumn("cart_to_purchase_rate", F.round(F.col("total_purchases") / F.col("total_carts"), 4))
 
    # top_products sẽ là bảng tổng hợp các sản phẩm bán chạy nhất, dùng cho dashboard, phân tích xu hướng và hiển thị giao diện chính của web 
    top_products = df_silver.filter(F.col("event_type") == "purchase") \
        .groupBy("product_id").count() \
        .orderBy(F.desc("count")).limit(100) \
        .join(dim_products, on="product_id", how="inner") \
        .select("product_id", "display_name", "brand", "avg_price", "count")
 
    #  rfm sẽ là bảng tổng hợp điểm RFM (Recency, Frequency, Monetary) cho từng khách hàng, dùng để phân đoạn khách hàng  
    rfm = df_silver.groupBy("user_id").agg(
        F.datediff(F.lit(max_date), F.max("event_time")).alias("recency"),
        F.count(F.when(F.col("event_type") == "purchase", True)).alias("frequency"),
        F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)), 2).alias("monetary")
    ).withColumn("customer_segment", 
        F.when((F.col("monetary") > 500) & (F.col("frequency") > 5), "VIP")
         .when(F.col("monetary") > 300, "Loyal")
         .when(F.col("recency") > 30, "At Risk")
         .otherwise("Standard"))
 
    # Tạo database gold_db nếu chưa tồn tại và lưu các bảng đã xử lý vào Delta Lake để Metabase có thể query trực tiếp phục vụ BI 
    spark.sql("CREATE DATABASE IF NOT EXISTS gold_db")
    
    # Thêm bảng fact_events để Metabase query báo cáo
    fact_events = df_silver.select("event_time", "event_type", "product_id", "user_id", "price", "user_session")

    output_tables = {
        "gold_db.dim_products": dim_products,
        "gold_db.fact_events": fact_events,
        "gold_db.user_interactions": interaction_matrix,
        "gold_db.behavior_funnel": funnel_analysis,
        "gold_db.top_trending": top_products,
        "gold_db.user_rfm": rfm
    }

    for table_name, df in output_tables.items():
        print(f"Persisting table: {table_name}")
        df.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true") \
          .option("path", f"s3a://gold/{table_name.split('.')[1]}") \
          .saveAsTable(table_name)

    print("Quá trình xử lý tầng Gold hoàn tất")

if __name__ == "__main__":
    transform_gold()