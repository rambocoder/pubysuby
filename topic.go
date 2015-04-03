package pubysuby

import (
	"container/list"
	"log"
	"time"
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
	items          *list.List
}

func NewTopic(topicName string) *Topic {
	ch := make(chan topicRequest)
	t := Topic{
		CommandChannel: ch,
		globalTimeOut:  30,
		topicName:      topicName,
		item_max_age:   70,
		maxItemsLength: 5,
		items:          list.New(),
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
				pubOnceListeners[cmd.topicReplyChannel] = false

			} else if cmd.Cmd == "pull" {

				//log.Println("Started pull")
				pubOnceListeners[cmd.topicReplyChannel] = true
				if t.items.Len() > 0 {

					results := make([]TopicItem, 0, t.items.Len())
					for e := t.items.Front(); e != nil; e = e.Next() {
						var item TopicItem
						item = e.Value.(TopicItem)
						results = append(results, item)
					}
					cmd.topicReplyChannel <- results
					//log.Println("Closed pull")
					// close it so that pull receive stops
					close(cmd.topicReplyChannel)
					delete(pubOnceListeners, cmd.topicReplyChannel)
				}

			} else if cmd.Cmd == "pullsince" {

				//log.Println("Started pullsince: ", cmd.since)
				pubOnceListeners[cmd.topicReplyChannel] = true
				if t.items.Len() > 0 {

					results := make([]TopicItem, 0, t.items.Len())
					for e := t.items.Front(); e != nil; e = e.Next() {
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
						delete(pubOnceListeners, cmd.topicReplyChannel)
					}

				}

			} else if cmd.Cmd == "unsubscribe" {
				_, present := pubOnceListeners[cmd.topicReplyChannel]
				if present {
					// log.Println("unsubscribed")
					close(cmd.topicReplyChannel)
					delete(pubOnceListeners, cmd.topicReplyChannel)
				} else {
					log.Panicln("Listener was already removed")
				}

			} else if cmd.Cmd == "pub" {

				lastMessageId++
				item := TopicItem{MessageId: lastMessageId, Message: cmd.content, CreatedTime: time.Now()}
				t.items.PushBack(item)

				cmd.topicReplyChannel <- []TopicItem{item}

				//fmt.Println("Publish", cmd.content)
				for ch, subOnce := range pubOnceListeners {
					ch <- []TopicItem{item}
					if subOnce {
						close(ch)
						delete(pubOnceListeners, ch)
					}

				}
			} else if cmd.Cmd == "lastMessageId" {
				cmd.topicReplyChannel <- []TopicItem{{MessageId: lastMessageId}}
			}
		} // end of select
	} // end of for
}

func (t *Topic) GC() {
	if t.items.Len() > 0 {
		trimToMaxAge(t.items, t.item_max_age)
		trimToSize(t.items, t.maxItemsLength)
	}

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
