package pubysuby

import (
	"time"
	//"fmt"
	//"log"
	"container/list"
	//"strconv"
)

type PubySuby struct {
	commandsChannel chan hubRequest
	globalTimeout   int64
}


type hubRequest struct {
	topic        string
	replyChannel chan chan TopicCommand
}

type TopicCommand struct {
	Cmd            string      // can be "sub" "subonce" "remove" "pub", "now"
	UniqueListener chan []TopicItem // filled in during "sub", "remove", "now"
	content        string      // message during "pub"
	since int64 // messageId during "pullsince"
}

type TopicItem struct {
	MessageId int64
	Message string
	CreatedTime time.Time
}

// New creates a new PubySuby and starts a goroutine for handling TopicCommands
func New() *PubySuby {
	ch := make(chan hubRequest)
	ps := PubySuby{commandsChannel : ch, globalTimeout: 30}
	go ps.hubController()
	return &ps
}

// Subscribe to all new messages for a topic
func (ps *PubySuby) Sub(topic string) chan []TopicItem {

	topicCommandChannel := ps.getTopicCommandChannel(topic)
	////fmt.Println("Received topic channel back")
	// once we have the topic channel, we have to send it our listener info
	myListenChannel := make(chan []TopicItem)

	topicCommandChannel <- TopicCommand{Cmd:"sub", UniqueListener: myListenChannel}

	return myListenChannel

}

func (ps *PubySuby) Unsubscribe( topic string, myListenChannel chan []TopicItem) {
	topicCommandChannel := ps.getTopicCommandChannel(topic)
	topicCommandChannel <- TopicCommand{Cmd:"remove", UniqueListener: myListenChannel}
}

// Pull all messages from the specified topic
// Blocks for the timeout duration in seconds until new message is published
func (ps *PubySuby) Pull(topic string, timeout int64) []TopicItem  {

	topicCommandChannel := ps.getTopicCommandChannel(topic)
	// once we have the topic channel, we have to send it our listener info

	myListenChannel := make(chan []TopicItem )

	topicCommandChannel <- TopicCommand{Cmd:"pull", UniqueListener: myListenChannel}
	var reply []TopicItem
	select {
	case results, ok := <-myListenChannel:
		if !ok {
			reply = nil
			//log.Fatalf("PullOnceWithTimeout melted down")
		} else
		{
			reply = results
		}
	case <- time.After(time.Second*time.Duration(timeout)):
		topicCommandChannel <- TopicCommand{Cmd:"remove", UniqueListener: myListenChannel}
		//log.Println(topic, "Timedout")
		reply = nil
	}
	return reply
}

// Pull all messages from the specified topic
// Blocks for the timeout duration in seconds until new message is published
func (ps *PubySuby) PullSince(topic string, timeout int64, since int64) []TopicItem  {

	topicCommandChannel := ps.getTopicCommandChannel(topic)
	// once we have the topic channel, we have to send it our listener info
	myListenChannel := make(chan []TopicItem )

	topicCommandChannel <- TopicCommand{Cmd:"pullsince", UniqueListener: myListenChannel, since: since}
	var reply []TopicItem
	select {
	case results, ok := <-myListenChannel:
		if !ok {
			reply = nil
			//log.Fatalf("PullSince melted down")
		} else
		{
			reply = results
		}
	case <- time.After(time.Second*time.Duration(timeout)):
		topicCommandChannel <- TopicCommand{Cmd:"remove", UniqueListener: myListenChannel}
		//log.Println(topic, "Timedout")
		reply = nil
	}
	return reply
}

// Publishes the message to the topic and returns the message id
func (ps *PubySuby) Push(topic string, message string) int64 {
	topicCommandChannel := ps.getTopicCommandChannel(topic)
	myListenChannel := make(chan []TopicItem )
	topicCommandChannel <- TopicCommand{Cmd:"pub", content: message, UniqueListener: myListenChannel}
	results, ok := <-myListenChannel
	if !ok {
		//fmt.Println(topic + "got closed explicitly in wait stage")
		return 0
	}
	return results[0].MessageId
}

// Retrieves the last message posted to the que
func (ps *PubySuby) LastMessageId(topic string) int64 {
	var topicCommandChannel chan TopicCommand
	topicCommandChannel = ps.getTopicCommandChannel(topic)
	////fmt.Println("Received topic channel back")
	// once we have the topic channel, we have to send it our listener info
	myListenChannel := make(chan []TopicItem)

	topicCommandChannel <- TopicCommand{Cmd:"lastMessageId", UniqueListener: myListenChannel}

	results, ok := <-myListenChannel
	if !ok {
		//fmt.Println(topic + "got closed explicitly in wait stage")
		return 0
	}
	return results[0].MessageId

}

func (ps *PubySuby) hubController() {
	// A topic name has a channel that can exchange topic commands
	// this list is thread safe because it resides in a goroutine
	topics := make(map[string] chan TopicCommand)
	for {
		var req hubRequest
		req = <-ps.commandsChannel // wait for commands here

		var requestReplyChannel chan chan TopicCommand
		requestReplyChannel = req.replyChannel

		// see if a channel for a topic name exists
		////fmt.Println("Fetch", req.topic)
		if (topics[req.topic] == nil) {
			// Add the following channel to the topic
			topicCommandChannel := make(chan TopicCommand)
			topics[req.topic] = topicCommandChannel
			// Start the topic handler in another thread
			go topicController(topicCommandChannel, req.topic, ps.globalTimeout, 70)
			// Send new topic channel info to the reply channel
			requestReplyChannel <- topicCommandChannel
		} else {
			// Send an existing topic channel to the reply channel
			requestReplyChannel <- topics[req.topic]
		}
	}
}

func topicController(tChan chan TopicCommand, topicName string, globalTimeout int64, item_max_age int) {
	//fmt.Println("Started topic controller", topicName)
	// key: listener channels that can receive a string
	// value: pub once
	listeners := make(map[chan []TopicItem] bool)
	var lastMessageId int64 = 0
	items := list.New()
	maxItemsLength := 5
	for
	{
		var cmd TopicCommand
		select {
		case <- time.After(time.Second*1):
			// Check if there are any expired messages every second
			trimToMaxAge(items, item_max_age)
			trimToSize(items, maxItemsLength)

		case cmd = <-tChan:
			if (cmd.Cmd == "sub") {

				//log.Println("Subscribed")
				listeners[cmd.UniqueListener] = false

			}else if (cmd.Cmd == "pull") {

				//log.Println("Started pull")
				listeners[cmd.UniqueListener] = true
				if (items.Len() > 0) {
					trimToMaxAge(items, item_max_age)
					trimToSize(items, maxItemsLength)
					results := make([]TopicItem, 0, items.Len())
					for e := items.Front(); e != nil; e = e.Next() {
						var item TopicItem
						item = e.Value.(TopicItem)
						results = append(results, item)
					}
					cmd.UniqueListener <- results
					//log.Println("Closed pull")
					close(cmd.UniqueListener)
					delete(listeners, cmd.UniqueListener)
				}

			} else if (cmd.Cmd == "pullsince") {

				//log.Println("Started pullsince: ", cmd.since)
				listeners[cmd.UniqueListener] = true
				if (items.Len() > 0) {
					trimToMaxAge(items, item_max_age)
					trimToSize(items, maxItemsLength)
					results := make([]TopicItem, 0, items.Len())
					for e := items.Front(); e != nil; e = e.Next() {
						var item TopicItem
						item = e.Value.(TopicItem)

						if (item.MessageId >  cmd.since) {
							results = append(results, item)
						}

					}
					if (len(results) > 0) {
						cmd.UniqueListener <- results
						//log.Println("Closed pull since")
						close(cmd.UniqueListener)
						delete(listeners, cmd.UniqueListener)
					}

				}

			}else if (cmd.Cmd == "remove") {
				_, present := listeners[cmd.UniqueListener]
				if (present) {
					//log.Println("unsubscribed")
					close(cmd.UniqueListener)
					delete(listeners, cmd.UniqueListener)
				} else {
					//fmt.Println("Listener was already removed")
				}


			} else if (cmd.Cmd == "pub") {

				lastMessageId++
				trimToMaxAge(items, item_max_age)
				trimToSize(items, maxItemsLength)
				item := TopicItem{MessageId: lastMessageId, Message: cmd.content, CreatedTime: time.Now()}
				items.PushBack(item)

				cmd.UniqueListener <- []TopicItem{item}

				//fmt.Println("Publish", cmd.content)
				for ch, subOnce := range listeners {
					ch <- []TopicItem{item}
					if subOnce {
						close(ch)
						delete(listeners, ch)
					}

				}
			} else if (cmd.Cmd == "lastMessageId") {
				cmd.UniqueListener <- []TopicItem{{MessageId: lastMessageId}}
			}

		}

	}

}

func (ps *PubySuby) getTopicCommandChannel(topicName string) chan TopicCommand {
	// Create a reply channel to get get channel back that can send commands to the topic controller
	replyChannel := make(chan chan TopicCommand)

	// Send the request to receive our topic
	ps.commandsChannel <- hubRequest{topic: topicName, replyChannel:replyChannel}

	// Receive the reply
	topicCommandChannel := <-replyChannel
	return topicCommandChannel
}

func trimToSize(l *list.List, size int) {
	if (l.Len() > size) {
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
		if (int(time.Since(item.CreatedTime).Seconds()) > seconds) {
			l.Remove(e)
		}

	}
}
