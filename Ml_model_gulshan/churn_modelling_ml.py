import numpy as np
import pandas as pd
import tensorflow as tf
import time
c=0;
while 1:

   if(c==0):

       c+=1
       dataset = pd.read_csv('C:/Users/HP/OneDrive/Desktop/Fraud_excell/Probable-exits-master/Churn_Modelling.csv')
       X = dataset.iloc[:, 3:-1].values
       y = dataset.iloc[:, -1].values
       # print(X)
       from sklearn.preprocessing import LabelEncoder

       le = LabelEncoder()
       X[:, 2] = le.fit_transform(X[:, 2])
       from sklearn.compose import ColumnTransformer
       from sklearn.preprocessing import OneHotEncoder

       ct = ColumnTransformer(transformers=[('encoder', OneHotEncoder(), [1])], remainder='passthrough')
       X = np.array(ct.fit_transform(X))
       # print(X)
       from sklearn.model_selection import train_test_split

       X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
       from sklearn.preprocessing import StandardScaler

       sc = StandardScaler()
       X_train = sc.fit_transform(X_train)
       X_test = sc.transform(X_test)
       print(X_test)
       ann = tf.keras.models.Sequential()
       ann.add(tf.keras.layers.Dense(units=6, activation='relu'))
       ann.add(tf.keras.layers.Dense(units=6, activation='relu'))

       ann.add(tf.keras.layers.Dense(units=1, activation='sigmoid'))
       ann.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

       ann.fit(X_train, y_train, batch_size=32, epochs=100)
   else:
       # further more this program would have to be running through whole time in background as it is real time implementation
       # we will be streaming data and if data is null then we will do continue; otherwise run the prediction of model
       # if(data==NULL)
       #  continue
       # else
       # make a prediction for an example of an out-of-sample observation
       print(ann.predict(sc.transform([[1, 0, 0, 600, 1, 40, 3, 60000, 2, 1, 1, 50000]])) > 0.5)
       time.sleep(1)