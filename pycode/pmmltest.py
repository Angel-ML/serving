import pandas as pd
import numpy as np

from sklearn.decomposition import PCA
from sklearn.feature_selection import SelectKBest
from sklearn.linear_model import LogisticRegression
from sklearn2pmml.pipeline import PMMLPipeline
from sklearn2pmml import sklearn2pmml

iris_df = pd.read_csv("data/iris/Iris.csv")
features = np.array(iris_df[["sepal.length", "sepal.width", "petal.length", "petal.width"]])
labels = np.array(iris_df["variety"].map({"Setosa": -1, "Versicolor": 1}))

# "Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width"
pipeline = PMMLPipeline([
    ("pca", PCA(n_components=3)),
    ("selector", SelectKBest(k=2)),
    ("classifier", LogisticRegression())
])

pipeline.fit(features, labels)

sklearn2pmml(pipeline, "LogisticRegressionIris.pmml", with_repr=True)
