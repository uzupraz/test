pipeline {
    agent any

    environment {
        IMAGE_NAME = 'elephanti-soft/interconnecthub-management-api'
        CONTAINER_NAME = 'interconnecthub-management-api'
        AWS_ACCESS_KEY_ID = credentials('jenkins-aws-secret-key-id')
        AWS_SECRET_ACCESS_KEY = credentials('jenkins-aws-secret-access-key')
        LAMBDA_NAME = 'interconnecthub-management-api'
        AWS_DEFAULT_REGION = 'eu-central-1'
    }

    stages {
        stage('Build Image') {
            when {
                anyOf { branch "PR-*"; branch "develop" }
            }
            steps {
                script {
                    sh "docker build -t ${IMAGE_NAME} ."
                }

            }
        }

        stage('Deploy Image') {
            when {
                branch "main"
            }
            steps {
                script {
                    // cleanup working directory output folder
                    sh "rm -f dist.zip"

                    // build and extract compiled files
                    sh "docker build -t ${IMAGE_NAME} ."
                    sh "docker create --name ${CONTAINER_NAME} ${IMAGE_NAME}"
                    sh "docker cp ${CONTAINER_NAME}:/app/dist.zip ."

                    // upload zip to lambda
                    sh "aws lambda update-function-code --function-name ${LAMBDA_NAME} --zip-file fileb://dist.zip"
                }
            }
        }

        stage('Cleanup Images') {
            steps{
                script {
                    sh "docker rm ${CONTAINER_NAME} || true"
                    sh "docker image rm ${IMAGE_NAME} || true"
                }
            }
        }
    }
}
