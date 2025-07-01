from tests.fr.hymaia.test_spark_utils import get_spark_session
import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo1.main import wordcount
from src.fr.hymaia.exo2.spark_clean_job import joindre, get_depart, ajouter_departement,filter_age
from src.fr.hymaia.exo2.spark_aggregate_job import compter_population_par_departement



class TestMain(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session("unit test")
    def test_wordcount(self):
        # GIVEN
        input = self.spark.createDataFrame(
            [
                Row(text='bonjour je suis un test unitaire'),
                Row(text='bonjour suis test')
            ]
        )
        expected = self.spark.createDataFrame(
            [
                Row(word='bonjour', count=2),
                Row(word='je', count=1),
                Row(word='suis', count=2),
                Row(word='un', count=1),
                Row(word='test', count=2),
                Row(word='unitaire', count=1),
            ]
        )

        actual = wordcount(input, 'text')

        self.assertCountEqual(actual.collect(), expected.collect())
    
    def test_filter_age(self):
        input = self.spark.createDataFrame([
            Row(dumb = "ignore", age = 17),
            Row(dumb = "ignore", age = 18),
            Row(dumb = "ignore", age = 19),
        ])

        expected = self.spark.createDataFrame([
            Row(dumb = "ignore", age = 18),
            Row(dumb = "ignore", age = 19),
    ])

        actual = filter_age(input)
        self.assertCountEqual(actual.collect(), expected.collect())
    
    def test_joindre(self):

        df_client = self.spark.createDataFrame(
            [
                Row(name = "Bubu", age = 30, zip = "38000"),
                Row(name = "Bibi", age = 20, zip = "35000"),
                Row(name = "Bobo", age = 22, zip = "20190"),
            ]
        )
        df_cities = self.spark.createDataFrame(
            [
                Row(zip = "38000", city = "Grenoble"),
                Row(zip = "35000", city = "Rennes"),
                Row(zip = "20190", city = "Corse"),
            ]
        )
        expected = self.spark.createDataFrame(
            [
                Row(zip = "38000", name = "Bubu", age = 30, city = "Grenoble"),
                Row(zip = "35000", name = "Bibi", age = 20, city = "Rennes"),
                Row(zip = "20190", name = "Bobo", age = 22, city = "Corse"),
            ]
        )

        actual = joindre(df_client, df_cities)
        self.assertCountEqual(actual.collect(), expected.collect())
    
    def test_get_depart(self):
        self.assertEqual(get_depart("38000"), "38")
        self.assertEqual(get_depart("35000"), "35")
        self.assertEqual(get_depart("20200"), "2B")
        self.assertEqual(get_depart("20000"), "2A")
    
    def test_ajouter_departement(self):
        df_input = self.spark.createDataFrame([
            Row(name = "Bubu", age = 30, zip = "38000", city = "Grenoble"),
            Row(name = "Bibi", age = 20, zip = "35000", city = "Rennes"),
            Row(name = "Bobo", age = 22, zip = "20190", city = "Corse"),
        ])

        expected = self.spark.createDataFrame([
            Row(name = "Bubu", age = 30, zip = "38000", city = "Grenoble", departement = "38"),
            Row(name = "Bibi", age = 20, zip = "35000", city = "Rennes", departement = "35"),
            Row(name = "Bobo", age = 22, zip = "20190", city = "Corse", departement = "2A"),
        ])

        actual = ajouter_departement(df_input)

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_compter_population(self):
        df_input = self.spark.createDataFrame([
            Row(departement="75"),
            Row(departement="75"),
            Row(departement="2B"),
            Row(departement="33"),
        ])
        expected = self.spark.createDataFrame([
            Row(departement="75", nb_personnes=2),
            Row(departement="2B", nb_personnes=1),
            Row(departement="33", nb_personnes=1),
        ])

        actual = compter_population_par_departement(df_input)
        self.assertCountEqual(actual.collect(), expected.collect())