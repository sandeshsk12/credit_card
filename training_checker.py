from xml.dom.minicompat import defproperty
from model_training_scripts.training_preprocessor import preprocessor
from model_training_scripts.training import trainer
import numpy as np
import pandas as pd
import statistics
a = preprocessor()
b = trainer()

df = a.load_data()
# print(df)
df = a.check_empty_spaces_in_col_names(df)
# print(df)
features, target = a.seperate_target_features(
    df, target_variable='default_payment_next_month')
# print(features)
# print(target)

features.iloc[3, 5] = np.nan
missing_yes_no, total_missing, missing_cols = (a.check_missing(features))
print(missing_yes_no, total_missing, missing_cols)
features = a.impute_missing_values(features, [missing_cols])

missing_yes_no, total_missing, missing_cols = (a.check_missing(features))
print(missing_yes_no, total_missing, missing_cols)


col_without_variation = a.cols_without_deviation(features)
print(col_without_variation)

features = a.drop_cols_without_deviation(features, col_without_variation)
print(features.columns)

print(len(target[target['default_payment_next_month'] == 1]))
print(len(target[target['default_payment_next_month'] == 0]))

features = a.scale_numerical_cols(features)
# print(features)
X_train, X_test, y_train, y_test = b.data_splitter(features, target)
y_train = ((y_train.to_numpy()).reshape(-1))

x, y = a.handle_imbalanced_dataset(X_train, y_train)
print(x.columns)
print(len(y[y == 1]))
print(len(y[y == 1]))
print(len(y))
print(x)
model = b.trainer(x, y)

print(model)

""""

f=features[features['SEX']==1]
g=['yes','yes','yes','yes','yes','No']
print(statistics.stdev(g))
for col in features.columns:
    print(col)
    print(statistics.stdev(g))
    """
