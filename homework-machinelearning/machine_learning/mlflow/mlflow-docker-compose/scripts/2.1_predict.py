import mlflow

from sklearn.model_selection import train_test_split
from sklearn.datasets import load_diabetes

mlflow.set_tracking_uri("http://localhost:5000")

db = load_diabetes()
X_train, X_test, y_train, y_test = train_test_split(db.data, db.target)

logged_model = 'runs:/ff5159dd0edf43efae4b03914985ea38/model'

model = mlflow.sklearn.load_model(logged_model)
predictions = model.predict(X_test)
print(predictions)