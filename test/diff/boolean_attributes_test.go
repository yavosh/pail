package diff

func sqsBooleanAttributesScenario() scenario {
	const queue = `"QueueUrl":"{queueUrl}"`
	return scenario{name: "sqs-boolean-attributes", queues: []string{"{name}.fifo"}, steps: []step{
		sqsStep("create-capitalized", "CreateQueue", `{"QueueName":"{name}.fifo","Attributes":{"FifoQueue":"True","ContentBasedDeduplication":"False","SqsManagedSseEnabled":"False"}}`),
		sqsStep("get-created-attributes", "GetQueueAttributes", `{`+queue+`,"AttributeNames":["FifoQueue","ContentBasedDeduplication","SqsManagedSseEnabled"]}`),
		sqsStep("create-lowercase-again", "CreateQueue", `{"QueueName":"{name}.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"false","SqsManagedSseEnabled":"false"}}`),
		sqsStep("enable-capitalized", "SetQueueAttributes", `{`+queue+`,"Attributes":{"ContentBasedDeduplication":"True","SqsManagedSseEnabled":"True"}}`),
		sqsStep("get-enabled-attributes", "GetQueueAttributes", `{`+queue+`,"AttributeNames":["ContentBasedDeduplication","SqsManagedSseEnabled"]}`),
		sqsStep("send-content-dedup", "SendMessage", `{`+queue+`,"MessageBody":"body","MessageGroupId":"g"}`),
		sqsStep("disable-mixed-case", "SetQueueAttributes", `{`+queue+`,"Attributes":{"ContentBasedDeduplication":"fAlSe","SqsManagedSseEnabled":"FALSE"}}`),
		sqsStep("get-disabled-attributes", "GetQueueAttributes", `{`+queue+`,"AttributeNames":["ContentBasedDeduplication","SqsManagedSseEnabled"]}`),
		sqsStep("send-missing-dedup", "SendMessage", `{`+queue+`,"MessageBody":"other","MessageGroupId":"g"}`),
		sqsStep("reject-number", "SetQueueAttributes", `{`+queue+`,"Attributes":{"ContentBasedDeduplication":"1"}}`),
		sqsStep("reject-whitespace", "SetQueueAttributes", `{`+queue+`,"Attributes":{"ContentBasedDeduplication":" true"}}`),
		sqsStep("delete-queue", "DeleteQueue", `{`+queue+`}`),
	}}
}

func snsBooleanAttributesScenario() scenario {
	const topic = "&TopicArn={topicArn}"
	const sub = "&SubscriptionArn={subscriptionArn}"
	return scenario{name: "sns-boolean-attributes", topics: []string{"{name}.fifo", "{name}"}, queues: []string{"{name}"}, steps: []step{
		snsStep("create-capitalized", "Action=CreateTopic&Name={name}.fifo&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.value=True&Attributes.entry.2.key=ContentBasedDeduplication&Attributes.entry.2.value=False"+snsVersion),
		snsStep("get-created-attributes", "Action=GetTopicAttributes"+topic+snsVersion),
		snsStep("create-lowercase-again", "Action=CreateTopic&Name={name}.fifo&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.value=true&Attributes.entry.2.key=ContentBasedDeduplication&Attributes.entry.2.value=false"+snsVersion),
		snsStep("set-type-capitalized", "Action=SetTopicAttributes"+topic+"&AttributeName=FifoTopic&AttributeValue=True"+snsVersion),
		snsStep("enable-capitalized", "Action=SetTopicAttributes"+topic+"&AttributeName=ContentBasedDeduplication&AttributeValue=True"+snsVersion),
		snsStep("get-enabled-attributes", "Action=GetTopicAttributes"+topic+snsVersion),
		sqsStep("create-queue", "CreateQueue", `{"QueueName":"{name}"}`),
		sqsStep("get-queue-arn", "GetQueueAttributes", `{"QueueUrl":"{queueUrl}","AttributeNames":["QueueArn"]}`),
		sqsStep("allow-topic", "SetQueueAttributes", snsQueuePolicy),
		snsStep("subscribe-capitalized", "Action=Subscribe"+topic+"&Protocol=sqs&Endpoint={queueArn}&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=False"+snsVersion),
		snsStep("subscribe-lowercase-again", "Action=Subscribe"+topic+"&Protocol=sqs&Endpoint={queueArn}&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=false"+snsVersion),
		snsStep("get-created-subscription", "Action=GetSubscriptionAttributes"+sub+snsVersion),
		snsStep("enable-raw-capitalized", "Action=SetSubscriptionAttributes"+sub+"&AttributeName=RawMessageDelivery&AttributeValue=True"+snsVersion),
		snsStep("get-enabled-subscription", "Action=GetSubscriptionAttributes"+sub+snsVersion),
		snsStep("publish-content-dedup", "Action=Publish"+topic+"&Message=body&MessageGroupId=g"+snsVersion),
		sqsStep("receive-raw", "ReceiveMessage", `{"QueueUrl":"{queueUrl}","WaitTimeSeconds":10,"MaxNumberOfMessages":1}`),
		snsStep("disable-mixed-case", "Action=SetTopicAttributes"+topic+"&AttributeName=ContentBasedDeduplication&AttributeValue=fAlSe"+snsVersion),
		snsStep("get-disabled-attributes", "Action=GetTopicAttributes"+topic+snsVersion),
		snsStep("publish-missing-dedup", "Action=Publish"+topic+"&Message=other&MessageGroupId=g"+snsVersion),
		snsStep("reject-number", "Action=SetTopicAttributes"+topic+"&AttributeName=ContentBasedDeduplication&AttributeValue=1"+snsVersion),
		snsStep("reject-whitespace", "Action=SetSubscriptionAttributes"+sub+"&AttributeName=RawMessageDelivery&AttributeValue=+true"+snsVersion),
		snsStep("delete-fifo-topic", "Action=DeleteTopic"+topic+snsVersion),
		snsStep("create-standard-disabled", "Action=CreateTopic&Name={name}&Attributes.entry.1.key=FifoTopic&Attributes.entry.1.value=False&Attributes.entry.2.key=ContentBasedDeduplication&Attributes.entry.2.value=False"+snsVersion),
		snsStep("delete-standard-topic", "Action=DeleteTopic"+topic+snsVersion),
		sqsStep("delete-queue", "DeleteQueue", `{"QueueUrl":"{queueUrl}"}`),
	}}
}
