package pubysuby

import (
	"log"
	"time"
)

type PubySuby struct {
	hubRequests   chan hubRequest
	globalTimeout int64
}

type hubRequest struct {
	topicName       string
	hubReplyChannel chan chan topicRequest
}

// New creates a new PubySuby hub and
// starts a goroutine for handling commands
func NewPubySuby() *PubySuby {
	ch := make(chan hubRequest)
	ps := PubySuby{hubRequests: ch, globalTimeout: 30}
	go ps.hubController()
	return &ps
}

//func (ps *PubySuby) Stop() {
//	close(ps.hubRequests)
//}

type Subscription struct {
	TopicName     string
	ListenChannel chan []TopicItem
}

// Subscribe to all new messages for a topic
func (ps *PubySuby) Sub(topic string) *Subscription {

	topicCommandChannel := ps.getTopicRequestChannel(topic)
	////fmt.Println("Received topic channel back")
	// once we have the topic channel, we have to send it our listener info
	myListenChannel := make(chan []TopicItem)

	topicCommandChannel <- topicRequest{Cmd: "sub", subscriberListenChannel: myListenChannel}

	result := Subscription{
		TopicName:     topic,
		ListenChannel: myListenChannel,
	}
	return &result

}

//func (ps *PubySuby) SubWithStopChannel(topic string) (chan []TopicItem, chan int) {
//
//	topicCommandChannel := ps.getTopicRequestChannel(topic)
//	////fmt.Println("Received topic channel back")
//	// once we have the topic channel, we have to send it our listener info
//	myListenChannel := make(chan []TopicItem)
//
//	stopProducingChannel := make(chan int)
//	topicCommandChannel <- topicRequest{
//		Cmd: "sub",
//		topicReplyChannel: myListenChannel,
//		stopProducingChannel: stopProducingChannel}
//
//	return myListenChannel, stopProducingChannel
//
//}

func (ps *PubySuby) Unsubscribe(subscription *Subscription) {
	topicCommandChannel := ps.getTopicRequestChannel(subscription.TopicName)
	topicCommandChannel <- topicRequest{Cmd: "unsubscribe", subscriberListenChannel: subscription.ListenChannel}
}

// Pull all messages from the specified topic
// If none are in the topic, blocks for the timeout duration in milliseconds until new message is published
func (ps *PubySuby) Pull(topic string, timeout int64) []TopicItem {
	if timeout < 1 {
		timeout = 1
	}
	myListenChannel := make(chan []TopicItem)
	defer drainRemaining(myListenChannel)
	topicCommandChannel := ps.getTopicRequestChannel(topic)
	// once we have the topic channel, we have to send it our listener info

	topicCommandChannel <- topicRequest{Cmd: "pull", subscriberListenChannel: myListenChannel}

	var receivedMessages []TopicItem
	select {
	case results, ok := <-myListenChannel:
		if !ok {
			log.Fatalf("PullOnceWithTimeout melted down")
		} else {
			receivedMessages = results
		}
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		go func() {
			topicCommandChannel <- topicRequest{Cmd: "unsubscribe", subscriberListenChannel: myListenChannel}
		}()

	}
	return receivedMessages
}

// Pull all messages from the specified topic
// If none are in the topic, blocks for the timeout duration in seconds until new message is published
func (ps *PubySuby) PullSince(topic string, timeout int64, since int64) []TopicItem {
	if timeout < 1 {
		timeout = 1
	}
	myListenChannel := make(chan []TopicItem)
	defer drainRemaining(myListenChannel)

	topicCommandChannel := ps.getTopicRequestChannel(topic)
	// once we have the topic channel, we have to send it our listener info
	topicCommandChannel <- topicRequest{Cmd: "pullsince", subscriberListenChannel: myListenChannel, since: since}
	var receivedMessages []TopicItem
	select {
	case results, ok := <-myListenChannel:
		if !ok {
			log.Fatal("Blew up during PullSince because myListenChannel was closed")
		} else {
			receivedMessages = results
		}
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		go func() {
			topicCommandChannel <- topicRequest{Cmd: "unsubscribe", subscriberListenChannel: myListenChannel}
		}()
		//log.Println(topic, "Timedout")
	}
	return receivedMessages
}

// Publishes the message to the topic and returns the message id
func (ps *PubySuby) Push(topic string, message string) int64 {
	topicCommandChannel := ps.getTopicRequestChannel(topic)
	myListenChannel := make(chan []TopicItem)
	topicCommandChannel <- topicRequest{Cmd: "pub", content: message, subscriberListenChannel: myListenChannel}
	results, ok := <-myListenChannel
	if !ok {
		log.Fatal("Blew up during Push because myListenChannel was closed")
	}
	return results[0].MessageId
}

// Retrieves the last message posted to the que
func (ps *PubySuby) LastMessageId(topic string) int64 {

	topicCommandChannel := ps.getTopicRequestChannel(topic)
	////fmt.Println("Received topic channel back")
	// once we have the topic channel, we have to send it our listener info
	myListenChannel := make(chan []TopicItem)

	topicCommandChannel <- topicRequest{Cmd: "lastMessageId", subscriberListenChannel: myListenChannel}

	results, ok := <-myListenChannel
	if !ok {
		log.Fatal("Blew up during LastMessageId because myListenChannel was closed")
	}
	return results[0].MessageId

}

func (ps *PubySuby) hubController() {
	// A topic name has a channel that can exchange topic commands

	topics := make(map[string]*Topic)
	for req := range ps.hubRequests {

		// see if a channel for a topic name exists
		////fmt.Println("Fetch", req.topic)
		if topics[req.topicName] == nil {
			// Add the following channel to the topic
			t := NewTopic(req.topicName)
			topics[req.topicName] = t
			// Send new topic channel info to the reply channel
			req.hubReplyChannel <- t.CommandChannel
		} else {
			// Send an existing topic channel to the reply channel
			req.hubReplyChannel <- topics[req.topicName].CommandChannel
		}
	}
}

func (ps *PubySuby) getTopicRequestChannel(topicName string) chan topicRequest {
	// Create a reply channel to get channel back that can send commands to the topic controller
	reply := make(chan chan topicRequest)

	// Send the request to receive our topic
	ps.hubRequests <- hubRequest{topicName: topicName, hubReplyChannel: reply}
	// Receive the reply
	topicCommandChannel := <-reply
	return topicCommandChannel
}

func drainRemaining(myListenChannel chan []TopicItem) {
	for _ = range myListenChannel {

	}
}
