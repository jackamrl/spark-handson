import os
import shutil
import glob
import unittest
from tests.fr.hymaia.test_spark_utils import get_spark_session
from src.fr.hymaia.exo2.spark_clean_job import main as clean_main
from src.fr.hymaia.exo2.spark_aggregate_job import main as aggregate_main

class TestIntegrationSparkJobs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session("integration test")

        # Chemins distincts pour input et output
        cls.input_path = "data/exo2/input"
        cls.clean_output_path = "data/exo2/clean"
        cls.aggregate_output_path = "data/exo2/aggregate"

        # Nettoyer uniquement les dossiers de sortie avant tests (sans supprimer input)
        for path in [cls.clean_output_path, cls.aggregate_output_path]:
            if os.path.exists(path):
                files = glob.glob(os.path.join(path, '*'))
                for f in files:
                    if os.path.isdir(f):
                        shutil.rmtree(f)
                    else:
                        os.remove(f)
        # Ne pas recréer manuellement les dossiers, Spark le fera

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        # Nettoyage optionnel des sorties après tests
        for path in [cls.clean_output_path, cls.aggregate_output_path]:
            if os.path.exists(path):
                files = glob.glob(os.path.join(path, '*'))
                for f in files:
                    if os.path.isdir(f):
                        shutil.rmtree(f)
                    else:
                        os.remove(f)

    def test_clean_job_integration(self):
        # Assure-toi que clean_main utilise bien self.input_path comme source d'entrée
        clean_main(input_path=self.input_path, output_path=self.clean_output_path)

        df_clean = self.spark.read.parquet(self.clean_output_path)
        self.assertGreater(df_clean.count(), 0)
        self.assertIn("zip", df_clean.columns)

    def test_aggregate_job_integration(self):
        # Même remarque : aggregate_main doit utiliser clean_output_path comme input
        clean_main(input_path=self.input_path, output_path=self.clean_output_path)
        aggregate_main(input_path=self.clean_output_path, output_path=self.aggregate_output_path)

        df_agg = self.spark.read.parquet(self.aggregate_output_path)  # parquet, pas csv
        self.assertGreater(df_agg.count(), 0)
        self.assertIn("departement", df_agg.columns)

        df_agg.show(truncate=False)
