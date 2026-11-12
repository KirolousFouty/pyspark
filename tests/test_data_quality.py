import pytest
from pyspark_learning.data_quality import DataQualityError, duplicate_count, require_columns, require_non_null


def test_quality_helpers(spark):
    frame = spark.createDataFrame([(1, "a"), (1, None)], ["id", "name"])
    assert duplicate_count(frame, ["id"]) == 1
    with pytest.raises(DataQualityError):
        require_columns(frame, ["missing"])
    with pytest.raises(DataQualityError):
        require_non_null(frame, "name")
