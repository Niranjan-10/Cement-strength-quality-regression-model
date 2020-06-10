"""
This is the Entry point for Training the Machine Learning Model.

Written By: Niranjan
Version: 1.0
Revisions: None

"""

# some necessary imports
from sklearn.model_selection import train_test_split
from application_logging.logger import App_Logger
from data_ingestion.data_loader import Data_Getter
from data_preprocessing.preprocessing import Preprocessor
from data_preprocessing import clustering
from best_model_finder import tuner
from File_operation import file_methods


class trainModel:
    """
        This class is use to train the datasets
    """
    def __init__(self):
        self.log_writer = App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", "a+")

    def trainingModel(self):
        # Logging the start of Training
        self.log_writer.log(self.file_object, "start of training")
        try:
            #get the data from the source
            data_getter = Data_Getter(self.file_object,self.log_writer)
            data = data_getter.get_data()

            """doing the data preprocessing"""
            preprocessor = Preprocessor(self.file_object,self.log_writer)

            # check if missing values are present in the dataset
            is_null_present,cols_with_missing_values=preprocessor.is_null_present(data)

            # if missing values are there, replace them appropriately.
            if(is_null_present):
                data=preprocessor.impute_missing_values(data) # missing value imputation

            # create separate features and labels
            X, Y = preprocessor.separate_label_feature(data, label_column_name='Concrete_compressive _strength')

            X = preprocessor.logTransformation(X)
            """ Applying the clustering approach"""

            kmeans=clustering.KMeansClustering(self.file_object,self.log_writer) # object initialization.
            number_of_clusters=kmeans.elbow_plot(X)
            
            #  using the elbow plot to find the number of optimum clusters

             # Divide the data into clusters
            X=kmeans.create_clusters(X,number_of_clusters)
            #create a new column in the dataset consisting of the corresponding cluster assignments.
            print('inside train model')
            print(Y)
            X['Labels']=Y
            print(type(X))

            
             # getting the unique clusters from our dataset
            list_of_clusters=X['Cluster'].unique()
            

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""
            for i in list_of_clusters:
                cluster_data=X[X['Cluster']==i] # filter the data for one cluster
                # Prepare the feature and Label columns
                cluster_features=cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label= cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3, random_state=30)

                x_train_scaled = preprocessor.standardScalingData(x_train)
                x_test_scaled = preprocessor.standardScalingData(x_test)

                model_finder=tuner.Model_Finder(self.file_object,self.log_writer) # object initialization

                #getting the best model for each of the clusters
                best_model_name,best_model=model_finder.get_best_model(x_train_scaled,y_train,x_test_scaled,y_test)

                #saving the best model to the directory.
                file_op = file_methods.File_Operation(self.file_object,self.log_writer)
                save_model=file_op.save_model(best_model,best_model_name+str(i))
            
            # logging the successful Training
            self.log_writer.log(self.file_object, 'Successful End of Training')
            self.file_object.close()


        except Exception:
            # logging the unsuccessful Training
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception