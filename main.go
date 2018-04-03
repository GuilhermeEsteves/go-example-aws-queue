package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var queueChannel = make(chan bool)

func main() {
	//Cria e busca o client
	svc, resultURL := getClientQueue()

	for {
		go func() { getMesageQueue(svc, resultURL) }()

		switch <-queueChannel {
		case true:
			queueChannel = make(chan bool)
			continue
		default:
			log.Fatal("Erro na fila :(")
		}
	}
}

func getClientQueue() (*sqs.SQS, *sqs.GetQueueUrlOutput) {
	queueName := "teste"

	//Cria uma nova sessao com a aws
	//pegando as credenciais de variavel de ambiente AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("sa-east-1")},
	)

	//Cria client para comunicação
	svc := sqs.New(sess)

	//Busca url da queue pelo o nome
	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		log.Fatal("Error to connect queue :(")
	}
	return svc, resultURL
}

func getMesageQueue(svc *sqs.SQS, ulr *sqs.GetQueueUrlOutput) {
	//Recebe mensagem da fila
	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl: ulr.QueueUrl,
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(36000),
		WaitTimeSeconds:     aws.Int64(10),
	})

	if err != nil {
		queueChannel <- false
		log.Fatal("Error read message")
	}

	fmt.Printf("%d Messages received.\n", len(result.Messages))
	//verifica se veio alguma mensagem
	if len(result.Messages) > 0 {

		fmt.Println(result.Messages)

		//Delete a mensagem
		resultDelete, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      ulr.QueueUrl,
			ReceiptHandle: result.Messages[0].ReceiptHandle,
		})

		if err != nil {
			fmt.Println("Delete Error", err)
			queueChannel <- false
			return
		}

		fmt.Println("Message Deleted", resultDelete)

		queueChannel <- true
	} else {
		queueChannel <- true
	}
}
