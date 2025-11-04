(venv) ➜  aws-stock-screener git:(main) ✗ sam deploy
/opt/homebrew/Cellar/aws-sam-cli/1.145.2/libexec/lib/python3.14/site-packages/samtranslator/compat.py:2: UserWarning: Core Pydantic V1 functionality isn't compatible with Python 3.14 or greater.
  from pydantic import v1 as pydantic

        Managed S3 bucket: aws-sam-cli-managed-default-samclisourcebucket-jtlyp2pf1dy0
        Auto resolution of buckets can be turned off by setting resolve_s3=False
        To use a specific S3 bucket, set --s3-bucket=<bucket_name>
        Above settings can be stored in samconfig.toml
                                                                                                                                                                                             
        File with same data already exists at stock-screener-test/36aef5ed424fbf2e85b16de2b8b0b840, skipping upload                                                                          
                                                                                                                                                                                             
        File with same data already exists at stock-screener-test/36aef5ed424fbf2e85b16de2b8b0b840, skipping upload                                                                          

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

        Uploading to stock-screener-test/fa39c5f5cb1f596cfc3dce63ed8b32bb.template  4501 / 4501  (100.00%)


Waiting for changeset to be created..

CloudFormation stack changeset
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Operation                                      LogicalResourceId                              ResourceType                                   Replacement                                  
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
+ Add                                          DataBucket                                     AWS::S3::Bucket                                N/A                                          
+ Add                                          DataCollectorFunctionDailySchedulePermission   AWS::Lambda::Permission                        N/A                                          
+ Add                                          DataCollectorFunctionDailySchedule             AWS::Events::Rule                              N/A                                          
+ Add                                          DataCollectorFunction                          AWS::Lambda::Function                          N/A                                          
+ Add                                          DataCollectorLogGroup                          AWS::Logs::LogGroup                            N/A                                          
+ Add                                          TestFunction                                   AWS::Lambda::Function                          N/A                                          
* Modify                                       LambdaExecutionRole                            AWS::IAM::Role                                 False                                        
* Modify                                       TestFunctionLogGroup                           AWS::Logs::LogGroup                            True                                         
- Delete                                       StockScreenerTestFunction                      AWS::Lambda::Function                          N/A                                          
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


Changeset created successfully. arn:aws:cloudformation:us-east-1:664751201943:changeSet/samcli-deploy1762238894/14282c99-30cd-4676-a562-e1859318dd79


Previewing CloudFormation changeset before deployment
======================================================
Deploy this changeset? [y/N]: y

2025-11-04 17:48:38 - Waiting for stack create/update to complete

CloudFormation events from stack operations (refresh every 5.0 seconds)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ResourceStatus                                 ResourceType                                   LogicalResourceId                              ResourceStatusReason                         
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE_IN_PROGRESS                             AWS::CloudFormation::Stack                     stock-screener-test                            User Initiated                               
CREATE_IN_PROGRESS                             AWS::S3::Bucket                                DataBucket                                     -                                            
CREATE_IN_PROGRESS                             AWS::S3::Bucket                                DataBucket                                     Resource creation Initiated                  
CREATE_COMPLETE                                AWS::S3::Bucket                                DataBucket                                     -                                            
UPDATE_IN_PROGRESS                             AWS::IAM::Role                                 LambdaExecutionRole                            -                                            
UPDATE_FAILED                                  AWS::IAM::Role                                 LambdaExecutionRole                            Resource handler returned message: "Resource 
                                                                                                                                             stock-screener-data-664751201943-us-east-1/* 
                                                                                                                                             must be in ARN format or "*". (Service: Iam, 
                                                                                                                                             Status Code: 400, Request ID:                
                                                                                                                                             edaa58ef-c2fc-4a30-8207-838c29774130) (SDK   
                                                                                                                                             Attempt Count: 1)" (RequestToken:            
                                                                                                                                             1fe3e933-3639-6d3e-deb2-a72ef459ed21,        
                                                                                                                                             HandlerErrorCode: InvalidRequest)            
UPDATE_ROLLBACK_IN_PROGRESS                    AWS::CloudFormation::Stack                     stock-screener-test                            The following resource(s) failed to update:  
                                                                                                                                             [LambdaExecutionRole].                       
UPDATE_IN_PROGRESS                             AWS::IAM::Role                                 LambdaExecutionRole                            -                                            
UPDATE_COMPLETE                                AWS::IAM::Role                                 LambdaExecutionRole                            -                                            
UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS   AWS::CloudFormation::Stack                     stock-screener-test                            -                                            
DELETE_IN_PROGRESS                             AWS::S3::Bucket                                DataBucket                                     -                                            
DELETE_COMPLETE                                AWS::S3::Bucket                                DataBucket                                     -                                            
UPDATE_ROLLBACK_COMPLETE                       AWS::CloudFormation::Stack                     stock-screener-test                            -                                            
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Error: Failed to create/update the stack: stock-screener-test, Waiter StackUpdateComplete failed: Waiter encountered a terminal failure state: For expression "Stacks[].StackStatus" we matched expected path: "UPDATE_ROLLBACK_COMPLETE" at least once
(venv) ➜  aws-stock-screener git:(main) ✗ sam build 
/opt/homebrew/Cellar/aws-sam-cli/1.145.2/libexec/lib/python3.14/site-packages/samtranslator/compat.py:2: UserWarning: Core Pydantic V1 functionality isn't compatible with Python 3.14 or greater.
  from pydantic import v1 as pydantic
Building codeuri: /Users/tim/CODE_PROJECTS/aws-stock-screener/src/data_collector runtime: python3.12 architecture: x86_64 functions: DataCollectorFunction, TestFunction                     
 Running PythonPipBuilder:ResolveDependencies                                                                                                                                                
 Running PythonPipBuilder:CopySource                                                                                                                                                         

Build Succeeded

Built Artifacts  : .aws-sam/build
Built Template   : .aws-sam/build/template.yaml

Commands you can use next
=========================
[*] Validate SAM template: sam validate
[*] Invoke Function: sam local invoke
[*] Test Function in the Cloud: sam sync --stack-name {{stack-name}} --watch
[*] Deploy: sam deploy --guided
(venv) ➜  aws-stock-screener git:(main) ✗ sam deploy
/opt/homebrew/Cellar/aws-sam-cli/1.145.2/libexec/lib/python3.14/site-packages/samtranslator/compat.py:2: UserWarning: Core Pydantic V1 functionality isn't compatible with Python 3.14 or greater.
  from pydantic import v1 as pydantic

        Managed S3 bucket: aws-sam-cli-managed-default-samclisourcebucket-jtlyp2pf1dy0
        Auto resolution of buckets can be turned off by setting resolve_s3=False
        To use a specific S3 bucket, set --s3-bucket=<bucket_name>
        Above settings can be stored in samconfig.toml
                                                                                                                                                                                             
        File with same data already exists at stock-screener-test/36aef5ed424fbf2e85b16de2b8b0b840, skipping upload                                                                          
                                                                                                                                                                                             
        File with same data already exists at stock-screener-test/36aef5ed424fbf2e85b16de2b8b0b840, skipping upload                                                                          

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

        Uploading to stock-screener-test/a4068d32efd5c0e48da4d5ef24346992.template  4541 / 4541  (100.00%)


Waiting for changeset to be created..

CloudFormation stack changeset
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Operation                                      LogicalResourceId                              ResourceType                                   Replacement                                  
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
+ Add                                          DataBucket                                     AWS::S3::Bucket                                N/A                                          
+ Add                                          DataCollectorFunctionDailySchedulePermission   AWS::Lambda::Permission                        N/A                                          
+ Add                                          DataCollectorFunctionDailySchedule             AWS::Events::Rule                              N/A                                          
+ Add                                          DataCollectorFunction                          AWS::Lambda::Function                          N/A                                          
+ Add                                          DataCollectorLogGroup                          AWS::Logs::LogGroup                            N/A                                          
+ Add                                          TestFunction                                   AWS::Lambda::Function                          N/A                                          
* Modify                                       LambdaExecutionRole                            AWS::IAM::Role                                 False                                        
* Modify                                       TestFunctionLogGroup                           AWS::Logs::LogGroup                            True                                         
- Delete                                       StockScreenerTestFunction                      AWS::Lambda::Function                          N/A                                          
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


Changeset created successfully. arn:aws:cloudformation:us-east-1:664751201943:changeSet/samcli-deploy1762239076/cb06f452-bfce-49d1-966a-65435fa36340


Previewing CloudFormation changeset before deployment
======================================================
Deploy this changeset? [y/N]: y

2025-11-04 17:51:32 - Waiting for stack create/update to complete

CloudFormation events from stack operations (refresh every 5.0 seconds)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ResourceStatus                                 ResourceType                                   LogicalResourceId                              ResourceStatusReason                         
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE_IN_PROGRESS                             AWS::CloudFormation::Stack                     stock-screener-test                            User Initiated                               
CREATE_IN_PROGRESS                             AWS::S3::Bucket                                DataBucket                                     -                                            
CREATE_IN_PROGRESS                             AWS::S3::Bucket                                DataBucket                                     Resource creation Initiated                  
CREATE_COMPLETE                                AWS::S3::Bucket                                DataBucket                                     -                                            
UPDATE_IN_PROGRESS                             AWS::IAM::Role                                 LambdaExecutionRole                            -                                            
UPDATE_COMPLETE                                AWS::IAM::Role                                 LambdaExecutionRole                            -                                            
CREATE_IN_PROGRESS                             AWS::Lambda::Function                          DataCollectorFunction                          -                                            
CREATE_IN_PROGRESS                             AWS::Lambda::Function                          TestFunction                                   -                                            
CREATE_IN_PROGRESS                             AWS::Lambda::Function                          TestFunction                                   Resource creation Initiated                  
CREATE_IN_PROGRESS                             AWS::Lambda::Function                          DataCollectorFunction                          Resource creation Initiated                  
CREATE_COMPLETE                                AWS::Lambda::Function                          TestFunction                                   -                                            
CREATE_COMPLETE                                AWS::Lambda::Function                          DataCollectorFunction                          -                                            
CREATE_IN_PROGRESS                             AWS::Logs::LogGroup                            DataCollectorLogGroup                          -                                            
CREATE_IN_PROGRESS                             AWS::Events::Rule                              DataCollectorFunctionDailySchedule             -                                            
UPDATE_IN_PROGRESS                             AWS::Logs::LogGroup                            TestFunctionLogGroup                           Requested update requires the creation of a  
                                                                                                                                             new physical resource; hence creating one.   
CREATE_IN_PROGRESS                             AWS::Logs::LogGroup                            DataCollectorLogGroup                          Resource creation Initiated                  
CREATE_IN_PROGRESS                             AWS::Events::Rule                              DataCollectorFunctionDailySchedule             Resource creation Initiated                  
UPDATE_IN_PROGRESS                             AWS::Logs::LogGroup                            TestFunctionLogGroup                           Resource creation Initiated                  
CREATE_COMPLETE                                AWS::Logs::LogGroup                            DataCollectorLogGroup                          -                                            
UPDATE_COMPLETE                                AWS::Logs::LogGroup                            TestFunctionLogGroup                           -                                            
CREATE_COMPLETE                                AWS::Events::Rule                              DataCollectorFunctionDailySchedule             -                                            
CREATE_IN_PROGRESS                             AWS::Lambda::Permission                        DataCollectorFunctionDailySchedulePermission   -                                            
CREATE_IN_PROGRESS                             AWS::Lambda::Permission                        DataCollectorFunctionDailySchedulePermission   Resource creation Initiated                  
CREATE_COMPLETE                                AWS::Lambda::Permission                        DataCollectorFunctionDailySchedulePermission   -                                            
UPDATE_COMPLETE_CLEANUP_IN_PROGRESS            AWS::CloudFormation::Stack                     stock-screener-test                            -                                            
DELETE_IN_PROGRESS                             AWS::Logs::LogGroup                            TestFunctionLogGroup                           -                                            
DELETE_COMPLETE                                AWS::Logs::LogGroup                            TestFunctionLogGroup                           -                                            
DELETE_IN_PROGRESS                             AWS::Lambda::Function                          StockScreenerTestFunction                      -                                            
DELETE_COMPLETE                                AWS::Lambda::Function                          StockScreenerTestFunction                      -                                            
UPDATE_COMPLETE                                AWS::CloudFormation::Stack                     stock-screener-test                            -                                            
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CloudFormation outputs from deployed stack
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Outputs                                                                                                                                                                                  
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Key                 DataBucket                                                                                                                                                           
Description         S3 bucket for CSV storage                                                                                                                                            
Value               stock-screener-data-664751201943-us-east-1                                                                                                                           

Key                 TestFunctionName                                                                                                                                                     
Description         Name of the test Lambda function                                                                                                                                     
Value               stock-screener-test-TestFunction-1qfd4vLpXQPN                                                                                                                        

Key                 DataCollectorFunctionName                                                                                                                                            
Description         Name of the data collector Lambda function                                                                                                                           
Value               stock-screener-test-DataCollectorFunction-WjPQ2JPCtbd5                                                                                                               

Key                 S3BucketName                                                                                                                                                         
Description         S3 bucket name for CSV files                                                                                                                                         
Value               stock-screener-data-664751201943-us-east-1                                                                                                                           
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


Successfully created/updated stack - stock-screener-test in us-east-1

(venv) ➜  aws-stock-screener git:(main) ✗ 