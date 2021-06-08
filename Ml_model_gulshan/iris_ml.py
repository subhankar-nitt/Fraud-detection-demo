

import numpy as np
import pandas as pd
# import seaborn as sns
# sns.set_palette('husl')
import matplotlib.pyplot as plt
# %matplotlib inline
import time
from sklearn import metrics
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
c=0;
while 1:
    if(c==0):
        c += 1
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

        plt.plot(k_range, scores)
        plt.xlabel('Value of k for KNN')
        plt.ylabel('Accuracy Score')
        plt.title('Accuracy Scores for Values of k of k-Nearest-Neighbors')
        plt.show()
        logreg = LogisticRegression()
        logreg.fit(X_train, y_train)
        y_pred = logreg.predict(X_test)
        print(metrics.accuracy_score(y_test, y_pred))
        knn = KNeighborsClassifier(n_neighbors=12)
        knn.fit(X, y)
    else:
        #further more this program would have to be running through whole time in background as it is real time implementation
        #we will be streaming data and if data is null then we will do continue; otherwise run the prediction of model
        #if(data==NULL)
        #  continue
        #else
          # make a prediction for an example of an out-of-sample observation
          #for now i put it to sleep
        print(knn.predict([[6, 3, 4, 2]]))
        time.sleep(1)


