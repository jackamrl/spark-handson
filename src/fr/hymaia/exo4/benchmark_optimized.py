"""
🚀 BENCHMARK ADAPTATIF - Exercice 4 Spark
Test automatique des 3 approches: Sans UDF, UDF Python, UDF Scala
"""
import subprocess
import time
import platform
import os
from datetime import datetime


def get_machine_specs():
    """Détection des specs machine (sans psutil)"""
    # Détection approximative de la RAM (fallback si pas d'info)
    try:
        # Linux: lecture de /proc/meminfo
        if platform.system() == "Linux":
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if 'MemTotal:' in line:
                        total_ram_kb = int(line.split()[1])
                        total_ram_gb = total_ram_kb // (1024 * 1024)
                        break
                else:
                    total_ram_gb = 8  # Fallback
        else:
            # Windows/Mac: estimation conservatrice  
            total_ram_gb = 8
    except:
        total_ram_gb = 8  # Fallback sécurisé
    
    # Détection CPU
    try:
        cpu_count = os.cpu_count() or 4
    except:
        cpu_count = 4
    
    os_name = platform.system()
    return total_ram_gb, cpu_count, os_name


def run_script(script_path, description):
    """Exécute un script et mesure son temps d'exécution"""
    print(f"\n{'='*70}")
    print(f"🚀 {description}")
    print(f"{'='*70}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            ["poetry", "run", "python", script_path],
            cwd=".",
            capture_output=True,
            text=True,
            timeout=1200  # 20 min timeout
        )
        
        execution_time = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ SUCCÈS - {execution_time:.1f}s")
            return execution_time, True, result.stdout
        else:
            print(f"❌ ERREUR - Code: {result.returncode}")
            print(f"📝 Stderr: {result.stderr[:500]}...")
            return execution_time, False, result.stderr
            
    except subprocess.TimeoutExpired:
        print("⏱️ TIMEOUT - Script trop long (>20 min)")
        return 1200, False, "TIMEOUT"
    except Exception as e:
        print(f"💥 EXCEPTION: {e}")
        return 0, False, str(e)


def extract_timings_from_output(output):
    """Extrait les timings détaillés depuis la sortie du script"""
    timings = {}
    lines = output.split('\n')
    
    for line in lines:
        if 'lignes lues en' in line:
            try:
                time_val = float(line.split('en ')[1].split('s')[0])
                timings['lecture'] = time_val
            except:
                pass
        elif 'Transformation' in line and 'cache en' in line:
            try:
                time_val = float(line.split('en ')[1].split('s')[0])
                timings['transformation'] = time_val
            except:
                pass
        elif 'Window functions en' in line:
            try:
                time_val = float(line.split('en ')[1].split('s')[0])
                timings['window_functions'] = time_val
            except:
                pass
        elif 'Temps total:' in line:
            try:
                time_val = float(line.split(': ')[1].split('s')[0])
                timings['total'] = time_val
            except:
                pass
                
    return timings


def main():
    # Affichage des specs machine
    total_ram_gb, cpu_count, os_name = get_machine_specs()
    
    print("🏆 BENCHMARK SPARK EXERCICE 4 - VERSION ADAPTATIVE")
    print("="*70)
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🖥️  Machine: {total_ram_gb}GB RAM, {cpu_count} CPU cores, {os_name}")
    print(f"📊 Volume: 60M lignes")
    
    # Scripts à tester
    scripts = [
        ("src/fr/hymaia/exo4/no_udf.py", "SANS UDF (Fonctions natives)"),
        ("src/fr/hymaia/exo4/python_udf.py", "UDF PYTHON"),
        ("src/fr/hymaia/exo4/scala_udf.py", "UDF SCALA"),
    ]
    
    results = {}
    
    # Exécution des tests
    for script_path, description in scripts:
        try:
            execution_time, success, output = run_script(script_path, description)
            
            if success:
                timings = extract_timings_from_output(output)
                results[description] = {
                    'time': execution_time,
                    'success': True,
                    'timings': timings
                }
            else:
                results[description] = {
                    'time': execution_time,
                    'success': False,
                    'error': output[:200]
                }
            
            # Pause entre tests
            if len(results) < len(scripts):
                print("💤 Pause 3s...")
                time.sleep(3)
                
        except KeyboardInterrupt:
            print("\n🛑 Benchmark interrompu")
            break
    
    # Affichage des résultats finaux
    print("\n" + "="*80)
    print("📊 RÉSULTATS FINAUX")
    print("="*80)
    
    # Tableau principal
    volume_lignes = 60_000_000
    successful_results = []
    
    for description, result in results.items():
        if result['success'] and 'timings' in result:
            timings = result['timings']
            total_time = timings.get('total', result['time'])
            debit = volume_lignes / total_time if total_time > 0 else 0
            
            print(f"\n🎯 {description}")
            print(f"   ⏱️  Temps total: {total_time:.1f}s")
            print(f"   📈 Débit: {debit:,.0f} lignes/sec")
            
            if 'lecture' in timings:
                print(f"   📖 Lecture: {timings['lecture']:.1f}s ({timings['lecture']/total_time*100:.0f}%)")
            if 'transformation' in timings:
                print(f"   🔧 Transformation: {timings['transformation']:.1f}s ({timings['transformation']/total_time*100:.0f}%)")
            if 'window_functions' in timings:
                print(f"   🪟 Window functions: {timings['window_functions']:.1f}s ({timings['window_functions']/total_time*100:.0f}%)")
                
            successful_results.append((description, total_time, debit))
        else:
            print(f"\n❌ {description}: ERREUR")
    
    # Classement performance
    if len(successful_results) > 1:
        print(f"\n🏆 CLASSEMENT PERFORMANCE:")
        sorted_results = sorted(successful_results, key=lambda x: x[1])  # Tri par temps
        
        for i, (desc, time_val, debit) in enumerate(sorted_results):
            medal = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i+1}."
            print(f"   {medal} {desc}: {time_val:.1f}s ({debit:,.0f} lignes/sec)")
        
        # Comparaisons relatives
        if len(sorted_results) >= 2:
            fastest = sorted_results[0]
            print(f"\n📈 GAINS vs le plus rapide:")
            for desc, time_val, debit in sorted_results[1:]:
                gain_percent = ((time_val - fastest[1]) / fastest[1]) * 100
                print(f"   • {desc}: +{gain_percent:.0f}% plus lent")
    
    # Résumé machine
    print(f"\n🖥️  RÉSUMÉ MACHINE:")
    print(f"   • RAM: {total_ram_gb}GB")
    print(f"   • CPU: {cpu_count} cores") 
    print(f"   • OS: {os_name}")
    print(f"   • Config Spark: Adaptative selon RAM")
    
    print("\n🏁 Benchmark terminé !")


if __name__ == "__main__":
    main() 