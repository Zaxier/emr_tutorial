# Essentially using this tutorial
# https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html
# 

##################################################
# STEP 1: Plan and configure an Amazon EMR cluster
##################################################

    # 1.1 Prepare storage for EMR cluster
    # ----------------------------------
        # Create bucket.
        aws s3api create-bucket --bucket emr-tutorial-xa --region us-east-1
        # >>>> RETURNS:
            # {
            #     "Location": "/emr-tutorial-xa"
            # }

        # Download data and extract data from:
        # https://docs.aws.amazon.com/emr/latest/ManagementGuide/samples/food_establishment_data.zip

        # Copy local data file to S3.
        aws s3 cp \
        food_establishment_data.csv \
        s3://emr-tutorial-xa/food_establishment_data.csv

        # Copy pyspark code to S3.
        aws s3 cp \
        example_pyspark_health_violations.py \
        s3://emr-tutorial-xa/example_pyspark_health_violations.py


    # 1.2 Prepare the application
    # --------------------------
        # See the following files for application details
        #   - example_pyspark_health_violations.py
        #   - use_config.py and config.cfg are copied in and may use later
        #     (not essential)



    # 1.3 Launch EMR cluster
    # --------------------------------
        # Prep:
            # Make sure you already have a EC2 Key Pair and PEM file
            # see https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair
            # see https://www.notion.so/Create-an-Amazon-EC2-Key-Pair-and-PEM-File-67462f346156411ca3796466802d50dc
            # Set the permissions on the PEM file to allow you to log in directly 
            # to the master node of your running cluster.
            chmod og-rwx myKeyPair.pem


        # (I did it through the console but you can do it using cli).
        # See:
        #   - https://www.notion.so/Launch-an-EMR-cluster-08c0c2aebe384a5f9b57cef48e10e76a
        #   - https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html


########################################
# STEP 2: Manage your Amazon EMR cluster
########################################

    # 2.1 Submit work to Amazon EMR
    # -----------------------------
        # 2.1.1 Make sure you have the clusterId and that your cluster is
        # in WAITING mode
            aws emr list-clusters --cluster-states WAITING
            # RETURNS:
                # {
                # "Clusters": [
                #     {
                #         "Id": "j-CPXZN7O7N65R",
                #         "Name": "myCluster",
                #         "Status": {
                #             "State": "WAITING", ### SEE WAITING
                #             "StateChangeReason": {
                #                 "Message": "Cluster ready after last step completed."
                #             },
                #             "Timeline": {
                #                 "CreationDateTime": "2022-01-15T15:55:06.741000+11:00",
                #                 "ReadyDateTime": "2022-01-15T16:00:21.183000+11:00"
                #             }
                #         },
                #         "NormalizedInstanceHours": 0,
                #         "ClusterArn": "arn:aws:elasticmapreduce:ap-southeast-2:644100745902:cluster/j-CPXZN7O7N65R"
                #     }
                # ]
                # }

        # 2.1.2 Submit .py job as a step with the add-steps command
            aws emr add-steps \
                --cluster-id j-CPXZN7O7N65R \
                --steps Type=Spark,Name="Test Spark application",ActionOnFailure=CONTINUE,Args=[s3://emr-tutorial-xa/example_pyspark_health_violations.py,--data_source,s3://emr-tutorial-xa/food_establishment_data.csv,--output_uri,s3://emr-tutorial-xa/MyOutputFolder]
            # RETURNS:
                # {
                #     "StepIds": [
                #         "s-WO2P2OB72DMP"
                #     ]
                # }


        # 2.1.3 Query the status of your step with the `describe-step` command
        #   The state of the step should change from PENDING to RUNNING to 
        #   COMPLETED as the step runs. It should take about a minute.
            aws emr describe-step --cluster-id j-CPXZN7O7N65R --step-id s-WO2P2OB72DMP
            # RETURNS:
                # {
                # "Step": {
                #     "Id": "s-WO2P2OB72DMP",
                #     "Name": "Test Spark application",
                #     "Config": {
                #         "Jar": "command-runner.jar",
                #         "Properties": {},
                #         "Args": [
                #             "spark-submit",
                #             "s3://emr-tutorial-xa/example_pyspark_health_violations.py",
                #             "--data_source",
                #             "s3://emr-tutorial-xa/food_establishment_data.csv",
                #             "--output_uri",
                #             "s3://emr-tutorial-xa/MyOutputFolder"
                #         ]
                #     },
                #     "ActionOnFailure": "CONTINUE",
                #     "Status": {
                #         "State": "PENDING",
                #         "StateChangeReason": {},
                #         "Timeline": {
                #             "CreationDateTime": "2022-01-15T16:57:28.330000+11:00"
                #         }
                #     }
                # }
                # }
    
    # 2.2 View results
    # ----------------
        # After a step runs successfully, you can view its output results 
        # in your S3 output folder

#####################################
# STEP 3: Clean up your EMR resources
#####################################
    
    # 3.1 Terminate your cluster
    aws emr terminate-clusters --cluster-ids j-CPXZN707N65R