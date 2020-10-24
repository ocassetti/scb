import scb.etl.transformers as trs
from scb.etl.geo_utils import geo_utils_register
from scb.etl.loaders import load_source
from scb.pipeline.base import BasePipeline


class Pipeline(BasePipeline):
    """
    Simple pipeline for loading and transforming
    """

    def run(self):
        geo_utils_register(self.spark)
        load_source(self.spark)
        trs.checkin_aggregation(self.spark)
        trs.simple_profile(self.spark)
        trs.simple_als(self.spark)
        trs.linked_profiles(self.spark)
