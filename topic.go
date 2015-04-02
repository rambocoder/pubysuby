package pubysuby

import (
	"time"
	//"fmt"
	//"log"
	"container/list"
	//"strconv"
)

type topicRequest struct {
	Cmd               string           // can be "sub" "subonce" "unsubscribe" "pub", "now"
	topicReplyChannel chan []TopicItem // filled in during "sub", "unsubscribe", "now"
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
}

func NewTopic(topicName string) *Topic {
	ch := make(chan topicRequest)
	t := Topic{CommandChannel: ch, globalTimeOut: 30, topicName: topicName, item_max_age: 70, maxItemsLength: 5}
	go t.topicController()
	return &t
}

func (t *Topic) topicController() {
	//fmt.Println("Started topic controller", topicName)
	// key: listener channels that can receive a string
	// value: pub once
	listeners := make(map[chan []TopicItem]bool)
	var lastMessageId int64 = 0
	items := list.New()
	for {
		var cmd topicRequest
		select {
		case <-time.After(time.Second * 1):
			// Check if there are any expired messages every second
			trimToMaxAge(items, t.item_max_age)
			trimToSize(items, t.maxItemsLength)

		case cmd = <-t.CommandChannel:
			if cmd.Cmd == "sub" {

				//log.Println("Subscribed")
				listeners[cmd.topicReplyChannel] = false

			} else if cmd.Cmd == "pull" {

				//log.Println("Started pull")
				listeners[cmd.topicReplyChannel] = true
				if items.Len() > 0 {
					trimToMaxAge(items, t.item_max_age)
					trimToSize(items, t.maxItemsLength)
					results := make([]TopicItem, 0, items.Len())
					for e := items.Front(); e != nil; e = e.Next() {
						var item TopicItem
						item = e.Value.(TopicItem)
						results = append(results, item)
					}
					cmd.topicReplyChannel <- results
					//log.Println("Closed pull")
					close(cmd.topicReplyChannel)
					delete(listeners, cmd.topicReplyChannel)
				}

			} else if cmd.Cmd == "pullsince" {

				//log.Println("Started pullsince: ", cmd.since)
				listeners[cmd.topicReplyChannel] = true
				if items.Len() > 0 {
					trimToMaxAge(items, t.item_max_age)
					trimToSize(items, t.maxItemsLength)
					results := make([]TopicItem, 0, items.Len())
					for e := items.Front(); e != nil; e = e.Next() {
						var item TopicItem
						item = e.Value.(TopicItem)

						if item.MessageId > cmd.since {
							results = append(results, item)
						}

					}
					if len(results) > 0 {
						cmd.topicReplyChannel <- results
						//log.Println("Closed pull since")
						close(cmd.topicReplyChannel)
						delete(listeners, cmd.topicReplyChannel)
					}

				}

			} else if cmd.Cmd == "unsubscribe" {
				_, present := listeners[cmd.topicReplyChannel]
				if present {
					//log.Println("unsubscribed")
					close(cmd.topicReplyChannel)
					delete(listeners, cmd.topicReplyChannel)
				} else {
					//fmt.Println("Listener was already removed")
				}

			} else if cmd.Cmd == "pub" {

				lastMessageId++
				trimToMaxAge(items, t.item_max_age)
				trimToSize(items, t.maxItemsLength)
				item := TopicItem{MessageId: lastMessageId, Message: cmd.content, CreatedTime: time.Now()}
				items.PushBack(item)

				cmd.topicReplyChannel <- []TopicItem{item}

				//fmt.Println("Publish", cmd.content)
				for ch, subOnce := range listeners {
					ch <- []TopicItem{item}
					if subOnce {
						close(ch)
						delete(listeners, ch)
					}

				}
			} else if cmd.Cmd == "lastMessageId" {
				cmd.topicReplyChannel <- []TopicItem{{MessageId: lastMessageId}}
			}

		}

	}

}
