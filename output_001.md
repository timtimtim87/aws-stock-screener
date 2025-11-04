(venv) ➜  aws-stock-screener git:(main) ✗ sam deploy --guided
/opt/homebrew/Cellar/aws-sam-cli/1.145.2/libexec/lib/python3.14/site-packages/samtranslator/compat.py:2: UserWarning: Core Pydantic V1 functionality isn't compatible with Python 3.14 or greater.
  from pydantic import v1 as pydantic

Configuring SAM deploy
======================

        Looking for config file [samconfig.toml] :  Not found

        Setting default arguments for 'sam deploy'
        =========================================
        Stack Name [sam-app]: stock-screener-test    
        AWS Region [us-east-1]: us-east-1
        Parameter Environment [dev]: dev
        #Shows you resources changes to be deployed and require a 'Y' to initiate deploy
        Confirm changes before deploy [y/N]: y
        #SAM needs permission to be able to create roles to connect to the resources in your template
        Allow SAM CLI IAM role creation [Y/n]: y
        #Preserves the state of previously provisioned resources when an operation fails
        Disable rollback [y/N]: n
        Save arguments to configuration file [Y/n]: y
        SAM configuration file [samconfig.toml]:               
        SAM configuration environment [default]: 

        Looking for resources needed for deployment:

        Managed S3 bucket: aws-sam-cli-managed-default-samclisourcebucket-jtlyp2pf1dy0
        Auto resolution of buckets can be turned off by setting resolve_s3=False
        To use a specific S3 bucket, set --s3-bucket=<bucket_name>
        Above settings can be stored in samconfig.toml

        Saved arguments to config file
        Running 'sam deploy' for future deployments will use the parameters saved above.
        The above parameters can be changed by modifying samconfig.toml
        Learn more about samconfig.toml syntax at 
        https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-config.html

        Uploading to stock-screener-test/998a60d660a911a18e2647545933a123  15145148 / 15145148  (100.00%)

        Deploying with following values
        ===============================
        Stack name                   : stock-screener-test
        Region                       : us-east-1
        Confirm changeset            : True
        Disable rollback             : False
        Deployment s3 bucket         : aws-sam-cli-managed-default-samclisourcebucket-jtlyp2pf1dy0
        Capabilities                 : ["CAPABILITY_IAM"]
        Parameter overrides          : {"Environment": "dev"}
        Signing Profiles             : {}

Initiating deployment
=====================

        Uploading to stock-screener-test/7b1339b1980eb0c819068f0b71199263.template  2322 / 2322  (100.00%)


Waiting for changeset to be created..

CloudFormation stack changeset
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Operation                                      LogicalResourceId                              ResourceType                                   Replacement                                  
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
+ Add                                          LambdaExecutionRole                            AWS::IAM::Role                                 N/A                                          
+ Add                                          StockScreenerTestFunction                      AWS::Lambda::Function                          N/A                                          
+ Add                                          TestFunctionLogGroup                           AWS::Logs::LogGroup                            N/A                                          
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


Changeset created successfully. arn:aws:cloudformation:us-east-1:664751201943:changeSet/samcli-deploy1762237411/dcdc5e65-e3e8-4be4-8b83-695ef346a820


Previewing CloudFormation changeset before deployment
======================================================
Deploy this changeset? [y/N]: y

2025-11-04 17:24:27 - Waiting for stack create/update to complete

CloudFormation events from stack operations (refresh every 5.0 seconds)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ResourceStatus                                 ResourceType                                   LogicalResourceId                              ResourceStatusReason                         
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE_IN_PROGRESS                             AWS::CloudFormation::Stack                     stock-screener-test                            User Initiated                               
CREATE_IN_PROGRESS                             AWS::IAM::Role                                 LambdaExecutionRole                            -                                            
CREATE_IN_PROGRESS                             AWS::IAM::Role                                 LambdaExecutionRole                            Resource creation Initiated                  
CREATE_COMPLETE                                AWS::IAM::Role                                 LambdaExecutionRole                            -                                            
CREATE_IN_PROGRESS                             AWS::Lambda::Function                          StockScreenerTestFunction                      -                                            
CREATE_IN_PROGRESS                             AWS::Lambda::Function                          StockScreenerTestFunction                      Resource creation Initiated                  
CREATE_IN_PROGRESS - CONFIGURATION_COMPLETE    AWS::Lambda::Function                          StockScreenerTestFunction                      Eventual consistency check initiated         
CREATE_IN_PROGRESS                             AWS::Logs::LogGroup                            TestFunctionLogGroup                           -                                            
CREATE_IN_PROGRESS                             AWS::Logs::LogGroup                            TestFunctionLogGroup                           Resource creation Initiated                  
CREATE_COMPLETE                                AWS::Lambda::Function                          StockScreenerTestFunction                      -                                            
CREATE_IN_PROGRESS - CONFIGURATION_COMPLETE    AWS::Logs::LogGroup                            TestFunctionLogGroup                           Eventual consistency check initiated         
CREATE_IN_PROGRESS - CONFIGURATION_COMPLETE    AWS::CloudFormation::Stack                     stock-screener-test                            Eventual consistency check initiated         
CREATE_COMPLETE                                AWS::Logs::LogGroup                            TestFunctionLogGroup                           -                                            
CREATE_COMPLETE                                AWS::CloudFormation::Stack                     stock-screener-test                            -                                            
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CloudFormation outputs from deployed stack
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Outputs                                                                                                                                                                                  
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Key                 TestFunctionName                                                                                                                                                     
Description         Name of the test Lambda function                                                                                                                                     
Value               stock-screener-test-StockScreenerTestFunction-4De5nLmDWdXE                                                                                                           

Key                 TestFunctionArn                                                                                                                                                      
Description         ARN of the test Lambda function                                                                                                                                      
Value               arn:aws:lambda:us-east-1:664751201943:function:stock-screener-test-StockScreenerTestFunction-4De5nLmDWdXE                                                            
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


Successfully created/updated stack - stock-screener-test in us-east-1

(venv) ➜  aws-stock-screener git:(main) ✗ 