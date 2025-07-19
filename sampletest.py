{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "7951014b-6f45-41d0-affe-da06c95f250b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "4.0.0\n"
     ]
    }
   ],
   "source": [
    "import pyspark\n",
    "print(pyspark.__version__)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "b78dadf7-b7b8-4b77-abab-b3868779baa1",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "659f7306-a7ea-4625-8acd-d5057016bbe5",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import col, sum as _sum, avg, count, month, year"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "1dd1dad5-ac12-4bc9-a2dc-401bc1ba4099",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create Spark session\n",
    "spark = SparkSession.builder \\\n",
    ".appName(\"CustomerTransactionAnalysis\") \\\n",
    ".getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "cdd7cc4e-caa7-4cb7-bc1f-a1d0ab2e4a5c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----+-----+--------------------+---------+--------------------+---------+------------+----------------+---------------+\n",
      "|YEAR|MONTH|            SUPPLIER|ITEM CODE|    ITEM DESCRIPTION|ITEM TYPE|RETAIL SALES|RETAIL TRANSFERS|WAREHOUSE SALES|\n",
      "+----+-----+--------------------+---------+--------------------+---------+------------+----------------+---------------+\n",
      "|2020|    1|REPUBLIC NATIONAL...|   100009| BOOTLEG RED - 750ML|     WINE|        0.00|             0.0|            2.0|\n",
      "|2020|    1|           PWSWN INC|   100024|MOMENT DE PLAISIR...|     WINE|        0.00|             1.0|            4.0|\n",
      "|2020|    1|RELIABLE CHURCHIL...|     1001|S SMITH ORGANIC P...|     BEER|        0.00|             0.0|            1.0|\n",
      "|2020|    1|LANTERNA DISTRIBU...|   100145|SCHLINK HAUS KABI...|     WINE|        0.00|             0.0|            1.0|\n",
      "|2020|    1|DIONYSOS IMPORTS INC|   100293|SANTORINI GAVALA ...|     WINE|        0.82|             0.0|            0.0|\n",
      "|2020|    1|KYSELA PERE ET FI...|   100641|CORTENOVA VENETO ...|     WINE|        2.76|             0.0|            6.0|\n",
      "|2020|    1|SANTA MARGHERITA ...|   100749|SANTA MARGHERITA ...|     WINE|        0.08|             1.0|            1.0|\n",
      "|2020|    1|BROWN-FORMAN BEVE...|     1008|JACK DANIELS COUN...|     BEER|        0.00|             0.0|            2.0|\n",
      "|2020|    1|  JIM BEAM BRANDS CO|    10103|KNOB CREEK BOURBO...|   LIQUOR|        6.41|             4.0|            0.0|\n",
      "|2020|    1|INTERNATIONAL CEL...|   101117|   KSARA CAB - 750ML|     WINE|        0.33|             1.0|            2.0|\n",
      "|2020|    1|HEAVEN HILL DISTI...|    10120|J W DANT BOURBON ...|   LIQUOR|        1.70|             1.0|            0.0|\n",
      "|2020|    1|BACCHUS IMPORTERS...|    10123|NOTEWORTHY SMALL ...|   LIQUOR|        1.02|             0.0|            0.0|\n",
      "|2020|    1|BACCHUS IMPORTERS...|    10124|NOTEWORTHY SMALL ...|   LIQUOR|        0.68|             0.0|            0.0|\n",
      "|2020|    1|BACCHUS IMPORTERS...|    10125|NOTEWORTHY SMALL ...|   LIQUOR|        0.34|             0.0|            0.0|\n",
      "|2020|    1|MONSIEUR TOUTON S...|   101346|ALSACE WILLIAM GE...|     WINE|        0.00|             0.0|            2.0|\n",
      "|2020|    1|THE COUNTRY VINTN...|   101486|POLIZIANO ROSSO M...|     WINE|        0.00|             0.0|            1.0|\n",
      "|2020|    1|THE COUNTRY VINTN...|   101532|HATSUMAGO SAKE JU...|     WINE|        0.34|             1.0|            1.0|\n",
      "|2020|    1|     ROYAL WINE CORP|   101664|RAMON CORDOVA RIO...|     WINE|        0.16|             0.0|            2.0|\n",
      "|2020|    1|REPUBLIC NATIONAL...|   101702|MANISCHEWITZ CREA...|     WINE|        0.00|             0.0|            1.0|\n",
      "|2020|    1|     ROYAL WINE CORP|   101753|BARKAN CLASSIC PE...|     WINE|        0.00|             0.0|            3.0|\n",
      "+----+-----+--------------------+---------+--------------------+---------+------------+----------------+---------------+\n",
      "only showing top 20 rows\n"
     ]
    }
   ],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "\n",
    "spark = SparkSession.builder \\\n",
    "    .appName(\"Load CSV\") \\\n",
    "    .getOrCreate()\n",
    "\n",
    "df = spark.read.csv(r\"D:\\python related codes\\python\\Warehouse_and_Retail_Sales.csv\", header=True, inferSchema=True)\n",
    "df.show()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "de631812-5ffe-427e-8011-46bbbc3fc020",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Clean data: Remove nulls\n",
    "df_clean = df.dropna(subset=[\"SUPPLIER\", \"ITEM CODE\", \"ITEM DESCRIPTION\"])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "949eefa6-8c2a-4008-bf70-dbf869ecbc7a",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "DataFrame[YEAR: int, MONTH: int, SUPPLIER: string, ITEM CODE: string, ITEM DESCRIPTION: string, ITEM TYPE: string, RETAIL SALES: string, RETAIL TRANSFERS: double, WAREHOUSE SALES: double]"
      ]
     },
     "execution_count": 10,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df_clean"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "bf8e26de-9d05-42ea-b9d9-0111d6b3daea",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.7"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
