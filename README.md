# labeling-lambda-function
# on AWS Lambda

## Install dependencies in AWS Lambda
Navigate to the project directory containing your lambda_function.py source code file.
Create a new directory named package into which you will install your dependencies.
```bash
pip install --target ./package pandas
```
Add the lambda_function.py to the package directory
Create a .zip file of this package directory. Do not zip the folder but the content of it.
Go to your AWS Lanbda function page.
In the Code Source part, select "Upload from .zip file"
Upload the package.zip file just created.
Deploy and Test the function.
