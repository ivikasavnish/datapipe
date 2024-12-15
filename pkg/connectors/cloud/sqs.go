package cloud

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQSConnector struct {
	BaseConnector
	Config SQSConfig
	client *sqs.SQS
	queueURL string
}

type SQSConfig struct {
	Region          string
	QueueName       string
	AccessKeyID     string
	SecretAccessKey string
	WaitTimeSeconds int64
}

func NewSQSConnector(config SQSConfig) *SQSConnector {
	return &SQSConnector{
		BaseConnector: BaseConnector{
			Name:        "AWS SQS",
			Description: "Amazon Simple Queue Service connector",
			Version:     "1.0.0",
			Type:        "cloud_messaging",
		},
		Config: config,
	}
}

func (s *SQSConnector) Connect() error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(s.Config.Region),
		Credentials: credentials.NewStaticCredentials(
			s.Config.AccessKeyID,
			s.Config.SecretAccessKey,
			"",
		),
	})
	if err != nil {
		return err
	}
	
	s.client = sqs.New(sess)
	
	// Get queue URL
	result, err := s.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(s.Config.QueueName),
	})
	if err != nil {
		return err
	}
	
	s.queueURL = *result.QueueUrl
	return nil
}

func (s *SQSConnector) Disconnect() error {
	// AWS SDK handles connection management
	return nil
}

func (s *SQSConnector) Read() (interface{}, error) {
	return nil, nil
}

func (s *SQSConnector) Write(data interface{}) error {
	return nil
}

func (s *SQSConnector) GetConfig() interface{} {
	return s.Config
}

// Additional SQS-specific methods
func (s *SQSConnector) SendMessage(messageBody string, delaySeconds int64, attributes map[string]string) (*sqs.SendMessageOutput, error) {
	msgAttrs := make(map[string]*sqs.MessageAttributeValue)
	for k, v := range attributes {
		msgAttrs[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	
	input := &sqs.SendMessageInput{
		DelaySeconds:      aws.Int64(delaySeconds),
		MessageAttributes: msgAttrs,
		MessageBody:      aws.String(messageBody),
		QueueUrl:         aws.String(s.queueURL),
	}
	
	return s.client.SendMessage(input)
}

func (s *SQSConnector) ReceiveMessages(maxMessages int64) ([]*sqs.Message, error) {
	input := &sqs.ReceiveMessageInput{
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(s.queueURL),
		MaxNumberOfMessages: aws.Int64(maxMessages),
		WaitTimeSeconds:     aws.Int64(s.Config.WaitTimeSeconds),
	}
	
	result, err := s.client.ReceiveMessage(input)
	if err != nil {
		return nil, err
	}
	
	return result.Messages, nil
}

func (s *SQSConnector) DeleteMessage(receiptHandle string) error {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	}
	
	_, err := s.client.DeleteMessage(input)
	return err
}

func (s *SQSConnector) PurgeQueue() error {
	input := &sqs.PurgeQueueInput{
		QueueUrl: aws.String(s.queueURL),
	}
	
	_, err := s.client.PurgeQueue(input)
	return err
}

func (s *SQSConnector) GetQueueAttributes() (*sqs.GetQueueAttributesOutput, error) {
	input := &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(s.queueURL),
		AttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
	}
	
	return s.client.GetQueueAttributes(input)
}

func (s *SQSConnector) SendMessageBatch(messages []string, attributes []map[string]string) (*sqs.SendMessageBatchOutput, error) {
	entries := make([]*sqs.SendMessageBatchRequestEntry, len(messages))
	
	for i, msg := range messages {
		msgAttrs := make(map[string]*sqs.MessageAttributeValue)
		if i < len(attributes) {
			for k, v := range attributes[i] {
				msgAttrs[k] = &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(v),
				}
			}
		}
		
		entries[i] = &sqs.SendMessageBatchRequestEntry{
			Id:                aws.String(fmt.Sprintf("msg-%d", i)),
			MessageBody:       aws.String(msg),
			MessageAttributes: msgAttrs,
		}
	}
	
	input := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(s.queueURL),
	}
	
	return s.client.SendMessageBatch(input)
}
