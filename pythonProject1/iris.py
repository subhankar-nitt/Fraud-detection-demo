
class models:
    def __init__(self,spark):

    def knnModel(self):
        data = pd.read_csv('C:/Users/HP/OneDrive/Desktop/Fraud_excell/iris.csv')
                X = data.drop(['Id', 'Species'], axis=1)
                y = data['Species']
                # print(X.head())
                print(X.shape)
                # print(y.head())
                print(y.shape)
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=5)
                print(X_train.shape)
                print(y_train.shape)
                print(X_test.shape)
                print(y_test.shape)

                k_range = list(range(1, 26))
                scores = []
                for k in k_range:
                    knn = KNeighborsClassifier(n_neighbors=k)
                    knn.fit(X_train, y_train)
                    y_pred = knn.predict(X_test)
                    scores.append(metrics.accuracy_score(y_test, y_pred))