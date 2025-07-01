import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import time
import platform
import os
import subprocess


def get_adaptive_spark_config():
    """Auto-détection des ressources pour configuration optimale (Linux/Windows/Mac)"""
    total_ram_gb = 8  # Fallback par défaut
    
    try:
        system = platform.system()
        
        if system == "Linux":
            # Linux: lecture de /proc/meminfo
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if 'MemTotal:' in line:
                        total_ram_kb = int(line.split()[1])
                        total_ram_gb = total_ram_kb // (1024 * 1024)
                        break
                        
        elif system == "Windows":
            # Windows: utilisation de wmic
            try:
                result = subprocess.run(['wmic', 'computersystem', 'get', 'TotalPhysicalMemory', '/value'], 
                                       capture_output=True, text=True, timeout=10)
                for line in result.stdout.split('\n'):
                    if 'TotalPhysicalMemory=' in line:
                        total_ram_bytes = int(line.split('=')[1].strip())
                        total_ram_gb = total_ram_bytes // (1024**3)
                        break
            except:
                # Fallback Windows avec systeminfo
                try:
                    result = subprocess.run(['systeminfo'], capture_output=True, text=True, timeout=15)
                    for line in result.stdout.split('\n'):
                        if 'Total Physical Memory:' in line:
                            # Parsing "Total Physical Memory: 16,384 MB" ou "Total Physical Memory: 16 384 Mo"
                            memory_str = line.split(':')[1].strip()
                            # Extraction des chiffres
                            memory_mb = int(''.join(filter(str.isdigit, memory_str.split()[0].replace(',', '').replace(' ', ''))))
                            total_ram_gb = memory_mb // 1024
                            break
                except:
                    total_ram_gb = 8  # Fallback Windows
                    
        elif system == "Darwin":  # macOS
            # Mac: utilisation de sysctl
            try:
                result = subprocess.run(['sysctl', 'hw.memsize'], capture_output=True, text=True, timeout=10)
                memory_bytes = int(result.stdout.split(':')[1].strip())
                total_ram_gb = memory_bytes // (1024**3)
            except:
                total_ram_gb = 8  # Fallback Mac
        else:
            total_ram_gb = 8  # Autre OS
            
    except Exception:
        total_ram_gb = 8  # Fallback général
    
    # Détection CPU
    try:
        cpu_count = os.cpu_count() or 4
    except:
        cpu_count = 4
    
    # Configuration adaptative selon la machine
    if total_ram_gb >= 24:
        driver_memory = "12g"
        max_result = "8g"
    elif total_ram_gb >= 16:
        driver_memory = "8g"
        max_result = "4g"
    elif total_ram_gb >= 8:
        driver_memory = "4g"
        max_result = "2g"
    else:
        driver_memory = "2g"
        max_result = "1g"
    
    return driver_memory, max_result, total_ram_gb, cpu_count

def create_adaptive_spark_session():
    """Configuration Spark adaptative selon les ressources machine"""
    driver_memory, max_result, total_ram_gb, cpu_count = get_adaptive_spark_config()
    
    return SparkSession.builder \
        .appName("exo4_no_udf_adaptive") \
        .master("local[*]") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.driver.maxResultSize", max_result) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def main():
    print("🚀 Exercice 4 - SANS UDF (Version Adaptative)")
    
    # Affichage des specs machine
    driver_memory, max_result, total_ram_gb, cpu_count = get_adaptive_spark_config()
    print(f"🖥️  Machine: {total_ram_gb}GB RAM, {cpu_count} CPU cores, {platform.system()}")
    print(f"⚙️  Config Spark: {driver_memory} driver, {max_result} max result")
    
    spark = create_adaptive_spark_session()
    
    try:
        timings = {}
        total_start = time.time()
        
        # 📖 ÉTAPE 1: Lecture des données
        step_start = time.time()
        print("📖 Lecture des données...")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv("src/resources/exo4/sell.csv")
        row_count = df.count()
        timings['lecture'] = time.time() - step_start
        print(f"   ✅ {row_count:,} lignes lues en {timings['lecture']:.1f}s")
        
        # 🔧 ÉTAPE 2: Transformation avec fonctions natives
        step_start = time.time()
        print("🔧 Ajout category_name (fonctions natives)...")
        df_with_category = df.withColumn(
            "category_name",
            f.when(f.col("category") < 6, "food").otherwise("furniture")
        ).cache()
        
        # Force le cache
        cached_count = df_with_category.count()
        timings['transformation'] = time.time() - step_start
        print(f"   ✅ Transformation + cache en {timings['transformation']:.1f}s")
        
        # 🪟 ÉTAPE 3: Window Functions
        step_start = time.time()
        print("🪟 Window functions...")
        
        # Window par catégorie/jour
        window_day = Window.partitionBy("category_name", "date")
        df_with_windows = df_with_category.withColumn(
            "total_price_per_category_per_day",
            f.sum("price").over(window_day)
        )
        
        # Window glissante 30 jours
        window_30 = Window.partitionBy("category_name").orderBy("date").rowsBetween(-30, 0)
        df_with_windows = df_with_windows.withColumn(
            "total_price_per_category_per_day_last_30_days",
            f.sum("price").over(window_30)
        )
        
        final_count = df_with_windows.count()
        timings['window_functions'] = time.time() - step_start
        print(f"   ✅ Window functions en {timings['window_functions']:.1f}s")
        
        # 🧹 Nettoyage
        df_with_category.unpersist()
        
        timings['total'] = time.time() - total_start
        
        # 📊 RÉSULTATS FINAUX
        print("\n" + "="*60)
        print("📊 RÉSULTATS - SANS UDF")
        print("="*60)
        print(f"⏱️  Temps total: {timings['total']:.2f}s")
        print(f"📈 Débit: {final_count/timings['total']:,.0f} lignes/sec")
        print(f"📋 Détail par étape:")
        print(f"   📖 Lecture: {timings['lecture']:.1f}s ({timings['lecture']/timings['total']*100:.1f}%)")
        print(f"   🔧 Transformation: {timings['transformation']:.1f}s ({timings['transformation']/timings['total']*100:.1f}%)")
        print(f"   🪟 Window functions: {timings['window_functions']:.1f}s ({timings['window_functions']/timings['total']*100:.1f}%)")
        print("="*60)
        
        return timings['total']
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 