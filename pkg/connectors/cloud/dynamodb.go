package cloud

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/ivikasavnish/datapipe/pkg/connectors"
)

type DynamoDBConnector struct {
	connectors.BaseConnector
	Config DynamoDBConfig
	client *dynamodb.DynamoDB
}

type DynamoDBConfig struct {
	Region          string
	TableName       string
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string // Optional, for local DynamoDB
}

func NewDynamoDBConnector(config DynamoDBConfig) *DynamoDBConnector {
	return &DynamoDBConnector{
		BaseConnector: connectors.BaseConnector{
			Name:        "AWS DynamoDB",
			Description: "Amazon DynamoDB connector",
			Version:     "1.0.0",
			Type:        "cloud_database",
		},
		Config: config,
	}
}

func (d *DynamoDBConnector) Connect() error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(d.Config.Region),
		Credentials: credentials.NewStaticCredentials(
			d.Config.AccessKeyID,
			d.Config.SecretAccessKey,
			"",
		),
		Endpoint: aws.String(d.Config.Endpoint),
	})
	if err != nil {
		return err
	}

	d.client = dynamodb.New(sess)

	// Verify table exists
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(d.Config.TableName),
	}

	_, err = d.client.DescribeTable(input)
	return err
}

func (d *DynamoDBConnector) Disconnect() error {
	// AWS SDK handles connection management
	return nil
}

func (d *DynamoDBConnector) Read() (interface{}, error) {
	return nil, nil
}

func (d *DynamoDBConnector) Write(data interface{}) error {
	return nil
}

func (d *DynamoDBConnector) GetConfig() interface{} {
	return d.Config
}

// Additional DynamoDB-specific methods
func (d *DynamoDBConnector) PutItem(item interface{}) error {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(d.Config.TableName),
	}

	_, err = d.client.PutItem(input)
	return err
}

func (d *DynamoDBConnector) GetItem(key map[string]interface{}) (map[string]interface{}, error) {
	av, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.GetItemInput{
		Key:       av,
		TableName: aws.String(d.Config.TableName),
	}

	result, err := d.client.GetItem(input)
	if err != nil {
		return nil, err
	}

	if result.Item == nil {
		return nil, nil
	}

	var item map[string]interface{}
	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	return item, err
}

func (d *DynamoDBConnector) Query(keyCondition string, expressionAttrValues map[string]interface{}) ([]map[string]interface{}, error) {
	values, err := dynamodbattribute.MarshalMap(expressionAttrValues)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(d.Config.TableName),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeValues: values,
	}

	result, err := d.client.Query(input)
	if err != nil {
		return nil, err
	}

	var items []map[string]interface{}
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &items)
	return items, err
}

func (d *DynamoDBConnector) Scan(filterExpression string, expressionAttrValues map[string]interface{}) ([]map[string]interface{}, error) {
	values, err := dynamodbattribute.MarshalMap(expressionAttrValues)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.ScanInput{
		TableName:                 aws.String(d.Config.TableName),
		FilterExpression:          aws.String(filterExpression),
		ExpressionAttributeValues: values,
	}

	result, err := d.client.Scan(input)
	if err != nil {
		return nil, err
	}

	var items []map[string]interface{}
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &items)
	return items, err
}

func (d *DynamoDBConnector) DeleteItem(key map[string]interface{}) error {
	av, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return err
	}

	input := &dynamodb.DeleteItemInput{
		Key:       av,
		TableName: aws.String(d.Config.TableName),
	}

	_, err = d.client.DeleteItem(input)
	return err
}

func (d *DynamoDBConnector) UpdateItem(key map[string]interface{}, updateExpression string, expressionAttrValues map[string]interface{}) error {
	keyAV, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return err
	}

	values, err := dynamodbattribute.MarshalMap(expressionAttrValues)
	if err != nil {
		return err
	}

	input := &dynamodb.UpdateItemInput{
		Key:                       keyAV,
		TableName:                 aws.String(d.Config.TableName),
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: values,
	}

	_, err = d.client.UpdateItem(input)
	return err
}
