package pubysuby

import (
	"container/list"
	//"log"
	"time"
	"log"
	"strconv"
)

type topicRequest struct {
	Cmd               string           // can be "sub" "subonce" "unsubscribe" "pub", "now"
	subscriberListenChannel chan []TopicItem // filled in during "sub", "unsubscribe", "now"
	content           string           // message during "pub"
	since             int64            // messageId during "pullsince"
}

type TopicItem struct {
	MessageId   int64
	Message     string
	CreatedTime time.Time
}

type Topic struct {
	topicName      string
	globalTimeOut  int64
	item_max_age   int
	maxItemsLength int
	CommandChannel chan topicRequest
	messages          *list.List
}

func NewTopic(topicName string) *Topic {
	ch := make(chan topicRequest)
	t := Topic{
		CommandChannel: ch,
		globalTimeOut:  30,
		topicName:      topicName,
		item_max_age:   1,
		maxItemsLength: 100,
		messages:          list.New(),
	}
	go t.topicController()
	return &t
}

func (t *Topic) topicController() {
	//fmt.Println("Started topic controller", topicName)
	// key: listener channels that can receive a string
	// value: pub once
	// this is needed because "pull*" command has a listener
	// that will disappear after it receives the data
	// unlike the "sub"
	pubOnceListeners := make(map[chan []TopicItem]bool)
	var lastMessageId int64 = 1

	for {
		select {
		case <-time.After(time.Second * 1):
			t.GC()
		case cmd := <-t.CommandChannel:
			if cmd.Cmd == "sub" {

				//log.Println("Subscribed")
				pubOnceListeners[cmd.subscriberListenChannel] = false

			} else if cmd.Cmd == "pull" {

				//log.Println("Started pull")
				pubOnceListeners[cmd.subscriberListenChannel] = true
				if t.messages.Len() > 0 {

					results := make([]TopicItem, 0, t.messages.Len())
					for e := t.messages.Front(); e != nil; e = e.Next() {
						var item TopicItem
						item = e.Value.(TopicItem)
						results = append(results, item)
					}
					cmd.subscriberListenChannel <- results
					//log.Println("Closed pull")
					// close it so that pull receive stops
					delete(pubOnceListeners, cmd.subscriberListenChannel)
					close(cmd.subscriberListenChannel)

				}

			} else if cmd.Cmd == "pullsince" {

				//log.Println("Started pullsince: ", cmd.since)

				pubOnceListeners[cmd.subscriberListenChannel] = true
				// check if there is any data to send on the initial subscription
				if t.messages.Len() > 0 {
					results := make([]TopicItem, 0, t.messages.Len())
					for e := t.messages.Front(); e != nil; e = e.Next() {
						var item TopicItem
						item = e.Value.(TopicItem)

						if item.MessageId > cmd.since {
							results = append(results, item)
						}
					}
					if len(results) > 0 {
						cmd.subscriberListenChannel <- results
						//log.Println("Closed pull since")
						delete(pubOnceListeners, cmd.subscriberListenChannel)
						// TODO: Does this really notify the subscriber that no more data is coming?
						close(cmd.subscriberListenChannel)
					}
				}
			} else if cmd.Cmd == "unsubscribe" {
				_, present := pubOnceListeners[cmd.subscriberListenChannel]
				if present {
					//log.Println("unsubscribed")
					delete(pubOnceListeners, cmd.subscriberListenChannel)
					// TODO: Does this really notify the subscriber that no more data is coming?
					close(cmd.subscriberListenChannel)
				}

			} else if cmd.Cmd == "pub" {

				lastMessageId++
				item := TopicItem{MessageId: lastMessageId, Message: cmd.content, CreatedTime: time.Now()}
				t.messages.PushBack(item)

				cmd.subscriberListenChannel <- []TopicItem{item}

				//fmt.Println("Publish", cmd.content)
				for ch, subOnce := range pubOnceListeners {
					ch <- []TopicItem{item}
					if subOnce {
						delete(pubOnceListeners, ch)
						close(ch)
					}
				}
			} else if cmd.Cmd == "lastMessageId" {
				cmd.subscriberListenChannel <- []TopicItem{{MessageId: lastMessageId}}
				close(cmd.subscriberListenChannel)
			}
		} // end of select
	} // end of for
}

func (t *Topic) GC() {
	// TODO: If the topic is too busy, GC based on <-timeafter will not kick in
	messagesCount := t.messages.Len()
	if messagesCount > 0 {
		log.Println("GC due to messages Count: " + strconv.Itoa(messagesCount))
		t.trimToMaxAge()
		t.trimToSize()
	}

}

func (t *Topic) trimToSize() {
	messagesCount := t.messages.Len()
	if messagesCount > t.maxItemsLength {
		diff := messagesCount - t.maxItemsLength
		for i := 0; i < diff; i++ {
			t.messages.Remove(t.messages.Front()) // Remove the first item from the que
		}
	}
}

func (t *Topic)trimToMaxAge() {
	for e := t.messages.Front(); e != nil; e = e.Next() {
		var item TopicItem
		item = e.Value.(TopicItem)
		if int(time.Since(item.CreatedTime).Seconds()) > t.item_max_age {
			t.messages.Remove(e)
		}
	}
}
