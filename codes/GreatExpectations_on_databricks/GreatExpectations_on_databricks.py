# Databricks notebook source
# MAGIC %md
# MAGIC # Great Expectations（GE） によるノートブック型環境（Databricks）でのデータ品質保証を実行する方法のまとめ

# COMMAND ----------

# MAGIC %md
# MAGIC ## Great Expectations とは

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Great Expectations (GE) とは、データに対する検証、ドキュメント化、および、プロファイリングにより、データ品質の保証と改善を支援する OSS の Python ライブラリである。
# MAGIC 
# MAGIC 
# MAGIC GE に関する基本的な記事として、次の記事が公開されている。
# MAGIC 
# MAGIC | #    | 記事                                                       | 概要                   |
# MAGIC | ---- | ------------------------------------------------------------ | ---------------------- |
# MAGIC | 1    | [Welcome](https://docs.greatexpectations.io/docs/)           | 概要                   |
# MAGIC | 2    | [Getting started with Great Expectations](https://docs.greatexpectations.io/docs/tutorials/getting_started/tutorial_overview) | チュートリアル         |
# MAGIC | 3    | [Glossary of Terms](https://docs.greatexpectations.io/docs/glossary) | 用語集                 |
# MAGIC | 4    | [Customize your deployment Great Expectations](https://docs.greatexpectations.io/docs/reference/customize_your_deployment) | 利用するための考慮事項 |
# MAGIC | 5    | [Explore Expectations](https://greatexpectations.io/expectations/) | Expectation 一覧       |
# MAGIC | 6    | [Community Page • Great Expectations](https://greatexpectations.io/community) | コミュニティ関連       |
# MAGIC | 7    | [Case studies from Great Expectations](https://greatexpectations.io/case-studies/) | ケーススタディ         |
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC GE を利用する基本的な手順は次のようになっている。データに対する品質保証条件を Expectations として定義に基づき、データソースへの検証を行い、検証結果をドキュメント化することができる。
# MAGIC 
# MAGIC 1. セットアップ
# MAGIC 2. データへの接続
# MAGIC 3. Expectations の作成
# MAGIC 4. データの検証
# MAGIC 
# MAGIC 
# MAGIC 検証できるデータソースには次のものがある。
# MAGIC 
# MAGIC - SQLAlchemy 経由によるデータベース
# MAGIC - Pandas Dataframe
# MAGIC - Spark Dataframe
# MAGIC 
# MAGIC データソースごとに利用できる Expection が異なり、次のドキュメントにて整理されている。
# MAGIC 
# MAGIC - [Expectation implementations by backend | Great Expectations](https://docs.greatexpectations.io/docs/reference/expectations/implemented_expectations/)
# MAGIC 
# MAGIC GE を利用する方法として、CLIによる方法とノートブック型環境による方法がある。ノートブック型環境による方法を実施する際には、次のドキュメントが参考となる。
# MAGIC 
# MAGIC -   [How to instantiate a Data Context without a yml file](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file/)
# MAGIC -   [How to quickly explore Expectations in a notebook | Great Expectations](https://docs.greatexpectations.io/docs/guides/miscellaneous/how_to_quickly_explore_expectations_in_a_notebook/)
# MAGIC -   [How to pass an in-memory DataFrame to a Checkpoint | Great Expectations](https://docs.greatexpectations.io/docs/guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint/)
# MAGIC 
# MAGIC 
# MAGIC データ検証後に、ドキュメントを作成するだけでなく、次のような [Action](https://docs.greatexpectations.io/docs/terms/action) を設定可能。
# MAGIC 
# MAGIC -   [How to trigger Email as a Validation Action](https://docs.greatexpectations.io/docs/guides/validation/validation_actions/how_to_trigger_email_as_a_validation_action)
# MAGIC -   [How to collect OpenLineage metadata using a Validation Action](https://docs.greatexpectations.io/docs/guides/validation/validation_actions/how_to_collect_openlineage_metadata_using_a_validation_action)
# MAGIC -   [How to trigger Opsgenie notifications as a Validation Action](https://docs.greatexpectations.io/docs/guides/validation/validation_actions/how_to_trigger_opsgenie_notifications_as_a_validation_action)
# MAGIC -   [How to trigger Slack notifications as a Validation Action](https://docs.greatexpectations.io/docs/guides/validation/validation_actions/how_to_trigger_slack_notifications_as_a_validation_action)
# MAGIC -   [How to update Data Docs after validating a Checkpoint](https://docs.greatexpectations.io/docs/guides/validation/validation_actions/how_to_update_data_docs_as_a_validation_action)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Greate Exceptions を利用するための事前準備

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 1. Great Expectations のインストール

# COMMAND ----------

# MAGIC %pip install great_expectations -q

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. データ（データフレーム）の準備

# COMMAND ----------

schema = '''
`VendorID` INT,
`tpep_pickup_datetime` TIMESTAMP,
`tpep_dropoff_datetime` TIMESTAMP,
`passenger_count` INT,
`trip_distance` DOUBLE,
`RatecodeID` INT,
`store_and_fwd_flag` STRING,
`PULocationID` INT,
`DOLocationID` INT,
`payment_type` INT,
`fare_amount` DOUBLE,
`extra` DOUBLE,
`mta_tax` DOUBLE,
`tip_amount` DOUBLE,
`tolls_amount` DOUBLE,
`improvement_surcharge` DOUBLE,
`total_amount` DOUBLE,
`congestion_surcharge` DOUBLE
'''

src_files = [
    "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz",
#     "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-02.csv.gz",
]

tgt_df = (
    spark
    .read
    .format("csv")
    .schema(schema)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(src_files)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 基本的なデータ品質検証の実施
# MAGIC 
# MAGIC 次の記事を参考にしている。
# MAGIC 
# MAGIC - [How to Use Great Expectations in Databricks | Great Expectations](https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_in_databricks/)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Great Expectations のセットアップ

# COMMAND ----------

import datetime

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

# COMMAND ----------

# 検証結果を永続的に保存する場合には、`/dbfs`ディレクトリに配置すること
root_directory = "/tmp/great_expectations"
root_directory_in_spark_api = f"file:{root_directory}"

# COMMAND ----------

# root_directory の初期化
dbutils.fs.rm(root_directory_in_spark_api, True)

try:
    # ディレクトリを確認
    display(dbutils.fs.ls(root_directory_in_spark_api))
except:
    print('Directory is empty.')

# COMMAND ----------

# Great expectaions 利用時のエントリーポイントである Data Context を定義
# https://docs.greatexpectations.io/docs/terms/data_context/

# great_expectations.yml を参照せずに定義を実施
data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# 利用状況の情報共有を提供を停止
# https://docs.greatexpectations.io/docs/reference/anonymous_usage_statistics/
context.anonymous_usage_statistics.enabled = False

# COMMAND ----------

# ディレクトリを確認
display(dbutils.fs.ls(root_directory_in_spark_api))

# COMMAND ----------

# MAGIC %md
# MAGIC ### データへの接続

# COMMAND ----------

datasource_name = "taxi_datasource"
dataconnector_name = "databricks_df"
data_asset_name = "nyctaxi_tripdata_yellow_yellow_tripdata"
tgt_deploy_env = "prod"

# COMMAND ----------

datasource_config = {
    # データソースを定義
    # https://docs.greatexpectations.io/docs/terms/datasource
    "name": datasource_name,
    "class_name": "Datasource",

    # execution_engine を定義
    # https://docs.greatexpectations.io/docs/terms/execution_engine/
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "SparkDFExecutionEngine",
    },

    # データコネクターを定義
    # https://docs.greatexpectations.io/docs/terms/data_connector/
    "data_connectors": {
        dataconnector_name: {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                "some_key_maybe_pipeline_stage",
                "some_other_key_maybe_run_id",
            ],
        }
    },
}
context.add_datasource(**datasource_config)

# COMMAND ----------

batch_request = RuntimeBatchRequest(
    datasource_name=datasource_name,
    data_connector_name=dataconnector_name,
    data_asset_name = data_asset_name,
    batch_identifiers={
        "some_key_maybe_pipeline_stage": tgt_deploy_env,
        "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": tgt_df},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expectations を作成

# COMMAND ----------

expectation_suite_name = "nyctaxi_tripdata_yellow_yellow_tripdata"

# COMMAND ----------

context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name,
    overwrite_existing=True,
)

# COMMAND ----------

# expectations にファイルが作成されたことを確認
expectations_file_path = f'{root_directory_in_spark_api}/expectations/{expectation_suite_name}.json'
print(dbutils.fs.head(expectations_file_path))

# COMMAND ----------

# Validator
# https://docs.greatexpectations.io/docs/terms/validator
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

# exception を追加する際に検証を実施しないように設定
validator.interactive_evaluation = False

# COMMAND ----------

# exception を追加
_ = validator.expect_column_values_to_not_be_null(
    column="passenger_count",
)

_ = validator.expect_column_values_to_be_between(
    column="congestion_surcharge",
    min_value=0,
    max_value=1000,
    meta={
        "notes": {
            "format": "markdown",
            "content": "Example notes about this expectation. **Markdown** `Supported`."
        }
    },
)

_ = validator.expect_column_values_to_be_between(
    column="passenger_count",
    min_value=0,
    max_value=1000,
)

# COMMAND ----------

# expectations を保存
validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

# expectations がファイルに追記されたことを確認
print(dbutils.fs.head(expectations_file_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### データの検証

# COMMAND ----------

checkpoint_config_name = "nyctaxi_tripdata_yellow_yellow_tripdata__checkpoint"

# COMMAND ----------

# チェックポイントを定義
checkpoint_config = {
    "name":checkpoint_config_name,
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "expectation_suite_name": expectation_suite_name,
    "run_name_template": "%Y%m%d-%H%M%S-yctaxi_tripdata_yellow_yellow_tripdata",
}
context.add_checkpoint(**checkpoint_config)

# COMMAND ----------

# checkpoints にファイルが作成されたことを確認
checkpoints_file_path = f'{root_directory_in_spark_api}/checkpoints/{checkpoint_config_name}.yml'
print(dbutils.fs.head(checkpoints_file_path))

# COMMAND ----------

checkpoint_result = context.run_checkpoint(
    checkpoint_name=checkpoint_config_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)

# COMMAND ----------

# uncommitted/validations にディレクトリが作成されたことを確認
checkpoints_file_path = f'{root_directory_in_spark_api}/uncommitted/validations/{expectation_suite_name}'
display(dbutils.fs.ls(checkpoints_file_path))

# uncommitted/data_docs/local_site にファイルとディレクトリが作成されたことを確認
checkpoints_file_path = f'{root_directory_in_spark_api}/uncommitted/data_docs/local_site'
display(dbutils.fs.ls(checkpoints_file_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 検証結果を確認

# COMMAND ----------

# 品質チェック結果を表示
checkpoint_result["success"]

# COMMAND ----------

# 品質チェック結果の HTML ファイルをを表示
first_validation_result_identifier = (
    checkpoint_result.list_validation_result_identifiers()[0]
)
first_run_result = checkpoint_result.run_results[first_validation_result_identifier]

docs_path = first_run_result['actions_results']['update_data_docs']['local_site']

html = dbutils.fs.head(docs_path,)

displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 品質エラーがある場合の動作検証

# COMMAND ----------

# エラーとなる expectation を追加
validator.expect_column_values_to_not_be_null(
    column="congestion_surcharge",
)

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

checkpoint_result = context.run_checkpoint(
    checkpoint_name=checkpoint_config_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)

# COMMAND ----------

# 品質チェック結果のを表示
checkpoint_result["success"]

# COMMAND ----------

# 品質チェック結果の HTML ファイルをを表示
first_validation_result_identifier = (
    checkpoint_result.list_validation_result_identifiers()[0]
)
first_run_result = checkpoint_result.run_results[first_validation_result_identifier]

docs_path = first_run_result['actions_results']['update_data_docs']['local_site']

html = dbutils.fs.head(docs_path,)

displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## データプロファイリング
# MAGIC 
# MAGIC 次の記事を参考にしている。
# MAGIC 
# MAGIC - [Great Expectation on Databricks. Run great_expectations on the hosted… | by Probhakar | Medium](https://probhakar-95.medium.com/great-expectation-on-databricks-8777042e00de)

# COMMAND ----------

from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.sparkdf_dataset import  SparkDFDataset
from great_expectations.render.renderer import *
from great_expectations.render.view import DefaultJinjaPageView

# COMMAND ----------

basic_dataset_profiler = BasicDatasetProfiler()

# COMMAND ----------

# creating GE wrapper around spark dataframe
from great_expectations.dataset.pandas_dataset import PandasDataset
gdf = PandasDataset(
    tgt_df
    .limit(1000)
    .toPandas()
) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 下記のコードにより、spark データフレームでも実行できるか、パフォーマンスに課題あり
# MAGIC 
# MAGIC ```python
# MAGIC from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
# MAGIC gdf = SparkDFDataset(
# MAGIC     tgt_df
# MAGIC     .limit(1000)
# MAGIC ) 
# MAGIC 
# MAGIC print(gdf.spark_df.count())
# MAGIC gdf.spark_df.display()
# MAGIC ```

# COMMAND ----------

# データを確認
print(gdf.count())
gdf.head()

# COMMAND ----------


from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler

# データをプロファイリング
expectation_suite, validation_result = gdf.profile(BasicDatasetProfiler)

# COMMAND ----------

from great_expectations.render.renderer import (
    ProfilingResultsPageRenderer,
    ExpectationSuitePageRenderer,
)
from great_expectations.render.view import DefaultJinjaPageView

profiling_result_document_content = ProfilingResultsPageRenderer().render(validation_result)
expectation_based_on_profiling_document_content = ExpectationSuitePageRenderer().render(expectation_suite)

# COMMAND ----------

profiling_result_document_content

# COMMAND ----------

# HTML を生成
profiling_result_HTML = DefaultJinjaPageView().render(profiling_result_document_content) # type string or str
expectation_based_on_profiling_HTML = DefaultJinjaPageView().render(expectation_based_on_profiling_document_content)

# COMMAND ----------

displayHTML(profiling_result_HTML)

# COMMAND ----------

# MAGIC %md
# MAGIC ## リソースのクリーンアップ

# COMMAND ----------

dbutils.fs.rm(root_directory_in_spark_api, True)
