package pubysuby

import (
	"time"
	//"fmt"
	//"log"
	"container/list"
	//"strconv"
)

type PubySuby struct {
	hubRequests   chan hubRequest
	globalTimeout int64
}

type hubRequest struct {
	topic           string
	hubReplyChannel chan chan topicRequest
}

// New creates a new PubySuby hub and
// starts a goroutine for handling commands
func New() *PubySuby {
	ch := make(chan hubRequest)
	ps := PubySuby{hubRequests: ch, globalTimeout: 30}
	go ps.hubController()
	return &ps
}

//func (ps *PubySuby) Stop() {
//	close(ps.hubRequests)
//}

// Subscribe to all new messages for a topic
func (ps *PubySuby) Sub(topic string) chan []TopicItem {

	topicCommandChannel := ps.getTopicRequestChannel(topic)
	////fmt.Println("Received topic channel back")
	// once we have the topic channel, we have to send it our listener info
	myListenChannel := make(chan []TopicItem)

	topicCommandChannel <- topicRequest{Cmd: "sub", topicReplyChannel: myListenChannel}

	return myListenChannel

}

func (ps *PubySuby) Unsubscribe(topic string, myListenChannel chan []TopicItem) {
	topicCommandChannel := ps.getTopicRequestChannel(topic)
	topicCommandChannel <- topicRequest{Cmd: "unsubscribe", topicReplyChannel: myListenChannel}
}

// Pull all messages from the specified topic
// If none are in the topic, blocks for the timeout duration in seconds until new message is published
func (ps *PubySuby) Pull(topic string, timeout int64) []TopicItem {

	topicCommandChannel := ps.getTopicRequestChannel(topic)
	// once we have the topic channel, we have to send it our listener info

	myListenChannel := make(chan []TopicItem)

	topicCommandChannel <- topicRequest{Cmd: "pull", topicReplyChannel: myListenChannel}
	var reply []TopicItem
	select {
	case results, ok := <-myListenChannel:
		if !ok {
			reply = nil
			//log.Fatalf("PullOnceWithTimeout melted down")
		} else {
			reply = results
		}
	case <-time.After(time.Second * time.Duration(timeout)):
		topicCommandChannel <- topicRequest{Cmd: "unsubscribe", topicReplyChannel: myListenChannel}
		//log.Println(topic, "Timedout")
		reply = nil
	}
	return reply
}

// Pull all messages from the specified topic
// If none are in the topic, blocks for the timeout duration in seconds until new message is published
func (ps *PubySuby) PullSince(topic string, timeout int64, since int64) []TopicItem {

	topicCommandChannel := ps.getTopicRequestChannel(topic)
	// once we have the topic channel, we have to send it our listener info
	myListenChannel := make(chan []TopicItem)

	topicCommandChannel <- topicRequest{Cmd: "pullsince", topicReplyChannel: myListenChannel, since: since}
	var reply []TopicItem
	select {
	case results, ok := <-myListenChannel:
		if !ok {
			reply = nil
			//log.Fatalf("PullSince melted down")
		} else {
			reply = results
		}
	case <-time.After(time.Second * time.Duration(timeout)):
		topicCommandChannel <- topicRequest{Cmd: "unsubscribe", topicReplyChannel: myListenChannel}
		//log.Println(topic, "Timedout")
		reply = nil
	}
	return reply
}

// Publishes the message to the topic and returns the message id
func (ps *PubySuby) Push(topic string, message string) int64 {
	topicCommandChannel := ps.getTopicRequestChannel(topic)
	myListenChannel := make(chan []TopicItem)
	topicCommandChannel <- topicRequest{Cmd: "pub", content: message, topicReplyChannel: myListenChannel}
	results, ok := <-myListenChannel
	if !ok {
		//fmt.Println(topic + "got closed explicitly in wait stage")
		return 0
	}
	return results[0].MessageId
}

// Retrieves the last message posted to the que
func (ps *PubySuby) LastMessageId(topic string) int64 {
	var topicCommandChannel chan topicRequest
	topicCommandChannel = ps.getTopicRequestChannel(topic)
	////fmt.Println("Received topic channel back")
	// once we have the topic channel, we have to send it our listener info
	myListenChannel := make(chan []TopicItem)

	topicCommandChannel <- topicRequest{Cmd: "lastMessageId", topicReplyChannel: myListenChannel}

	results, ok := <-myListenChannel
	if !ok {
		//fmt.Println(topic + "got closed explicitly in wait stage")
		return 0
	}
	return results[0].MessageId

}

func (ps *PubySuby) hubController() {
	// A topic name has a channel that can exchange topic commands

	topics := make(map[string]*Topic)
	for req := range ps.hubRequests {

		// see if a channel for a topic name exists
		////fmt.Println("Fetch", req.topic)
		if topics[req.topic] == nil {
			// Add the following channel to the topic
			t := NewTopic(req.topic)
			topics[req.topic] = t
			// Send new topic channel info to the reply channel
			req.hubReplyChannel <- t.CommandChannel
		} else {
			// Send an existing topic channel to the reply channel
			req.hubReplyChannel <- topics[req.topic].CommandChannel
		}
	}
}

func (ps *PubySuby) getTopicRequestChannel(topicName string) chan topicRequest {
	// Create a reply channel to get channel back that can send commands to the topic controller
	reply := make(chan chan topicRequest)

	// Send the request to receive our topic
	ps.hubRequests <- hubRequest{topic: topicName, hubReplyChannel: reply}

	// Receive the reply
	topicCommandChannel := <-reply
	return topicCommandChannel
}

func trimToSize(l *list.List, size int) {
	if l.Len() > size {
		diff := l.Len() - size
		for i := 0; i < diff; i++ {
			l.Remove(l.Front()) // Remove the first item from the que
		}
	}
}

func trimToMaxAge(l *list.List, seconds int) {
	for e := l.Front(); e != nil; e = e.Next() {
		var item TopicItem
		item = e.Value.(TopicItem)
		if int(time.Since(item.CreatedTime).Seconds()) > seconds {
			l.Remove(e)
		}

	}
}
