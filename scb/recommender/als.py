from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from scb.logger.common import Logger
from scb.pipeline.base import BasePipeline

LOGGER = Logger.get_default_logger(__name__)


class Pipeline(BasePipeline):
    """
    Simple pipeline for loading and transforming
    """

    def _load_data(self):
        self.checkins_als = self.spark.read.orc("data/checkins")

    def _train(self):
        """
        Simplistic ALS with default 80/20 split and rsme
        :return:
        """
        (training, test) = self.checkins_als.randomSplit([0.80, 0.20])
        als = ALS(userCol="user_id", itemCol="venue_id", ratingCol="rating",
                  coldStartStrategy="drop", nonnegative=True, implicitPrefs=False)

        param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 50, 75, 100]) \
            .addGrid(als.maxIter, [5, 50, 75, 100]) \
            .addGrid(als.regParam, [.01, .05, .1, .15]) \
            .build()
        # Define evaluator as RMSE
        evaluator = RegressionEvaluator(metricName="rmse",
                                        labelCol="rating",
                                        predictionCol="prediction")
        LOGGER.info(f"Num models to be tested using param_grid: {len(param_grid)}")

        CrossValidator(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=5)
        self.model = als.fit(training)
        predictions = self.model.transform(test)

        rmse = evaluator.evaluate(predictions)
        LOGGER.info(f"Root-mean-square error = {str(rmse)}")
        return rmse

    def run(self):
        self._load_data()
        self._train()
        self.model.save("als-recommendation")
